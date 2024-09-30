/**
 * A component that shows a summary of a particular version of a job stored in TRAC. This is used by both
 * the ObjectSummaryScene and the JobInfoModal. This component is for viewing in a browser, there is a sister
 * component called {@link JobViewerPdf} that is for viewing in a PDF. These two components need to be kept
 * in sync so if a change is made to one then it should be reflected in the other.
 *
 * @remarks
 * There are three types of job in TRAC, there is a subcomponent for each type that renders that job's specific
 * content.
 *
 * @remarks
 * This component can either be passed a tag selector as a prop or it can get the object details from the URL
 * and use that to form a tag selector.
 *
 * @module JobViewer
 * @category ObjectSummaryScene Component
 */

import {Alert} from "../../../components/Alert";
import {Button} from "../../../components/Button";
import {ButtonToDownloadJson} from "../../../components/ButtonToDownloadJson";
import {ButtonToDownloadPdf} from "../../../components/ButtonToDownloadPdf";
import {getTagSelectorFromUrlParameters} from "../../../utils/utils_trac_metadata";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {isAlertVariant} from "../../../utils/utils_trac_type_chckers";
import {IsLatestVersion} from "../../../components/IsLatestVersion";
import {JobViewerImportModel} from "./JobViewerImportModel";
import {JobViewerRunFlow} from "./JobViewerRunFlow";
import {JobViewerRunModel} from "./JobViewerRunModel";
import {Loading} from "../../../components/Loading";
import {MetadataViewerModal} from "../../../components/MetadataViewerModal";
import {ObjectDetails} from "../../../components/ObjectDetails";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import PropTypes from "prop-types";
import React, {useCallback, useEffect, useState} from "react";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../../config/config_trac_classifications";
import {useMetadataStore} from "../../../utils/utils_async";
import {useAppSelector} from "../../../../types/types_hooks";
import {useNavigate, useParams} from "react-router-dom";
import {Variants} from "../../../../types/types_general";
import {ButtonToEmailObjectLink} from "../../../components/ButtonToEmailObjectLink";
import {rewriteUrlOfObjectParam} from "../../../utils/utils_general";

// A lookup to convert a job's status into the right type of alert colour and status message
// This uses the config options
let jobStatusLookup: Record<string, { variant: Extract<Variants, "success" | "warning" | "danger" | "info">, message: string }> = {}

Types.tracJobStatuses.map(jobStatus => {
    const variant = isAlertVariant(jobStatus.details.variant) ? jobStatus.details.variant : "info"
    jobStatusLookup[jobStatus.value] = {variant: variant, message: jobStatus.details.message}
})

/**
 * An interface for the props of the JobViewer component.
 */
export interface Props {

    /**
     * Whether to look at the URL parameters to define the metadata to fetch.
     */
    getTagFromUrl: boolean
    /**
     * The tag selector for the job to show a summary of.
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

export const JobViewer = (props: Props) => {

    const {getTagFromUrl} = props

    // Get what we need from the store
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    // A hook from the React Router plugin that allows us to navigate
    const navigate = useNavigate()

    // Get the URL parameters that defines the object to view, using hook from the React Router plugin.
    let {objectId, objectVersion, tagVersion} = useParams();

    // If the URL is of the form "<app_url>/{object_id}" rather than "<app_url>/{object_id}/{object_version}/{tag_version}" then
    // redirect to the latter version but set the object_version and tag_version parameters to 'latest'
    useEffect(() => {

        let shouldNavigate = true

        if (getTagFromUrl && !props.tagSelector && objectVersion === undefined && tagVersion === undefined) {

            if (shouldNavigate) {
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.JOB].to}/${objectId}/latest/latest`)
            }

        } else if (getTagFromUrl && props.tagSelector && objectId === undefined && objectVersion === undefined && tagVersion === undefined) {

            // If we were provided with a tag we still want to update the URL to match the requested tag, but we need to avoid an infinite loop
            // so only do this when there are no URL parameters. If we do this when the parameters are set we continuously trigger re-renders that
            // cause the item to be re-fetched
            if (shouldNavigate) {
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.JOB].to}/${props.tagSelector.objectId}/${props.tagSelector.latestObject ? "latest" : props.tagSelector.objectVersion}/${props.tagSelector.latestTag ? "latest" : props.tagSelector.tagVersion}`)
            }
        }

        // Do not allow redirects if this component unmounts
        return () => {
            shouldNavigate = false
        }

    }, [getTagFromUrl, navigate, objectId, objectVersion, props.tagSelector, tagVersion])

    const urlDetails = !getTagFromUrl ? undefined : getTagSelectorFromUrlParameters({objectType: trac.ObjectType.JOB, objectId, objectVersion, tagVersion, searchAsOf})

    // If we receive a prop with a tag selector then user that, otherwise use the URL parameters, but only if there are no errors
    const tagSelector = props.tagSelector ? props.tagSelector : urlDetails && urlDetails?.errorMessages.length === 0 ? urlDetails?.tagSelector : undefined

    // Whether to show the modal containing info about the job.
    const [showJobModal, setShowJobModal] = useState<boolean>(false)

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting a Tag for an object in TRAC along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There is a callback function that is passed which runs whenever the tag is
    // updated
    const [isDownloading, tag, status] = useMetadataStore(tagSelector, {suppressError: true})

    if (urlDetails !== undefined && status === "failed") {
        urlDetails.errorMessages.push("That job object ID and/or version numbers do not exist.")
    }

    // A hook that updates the URL in the browser to remove 'latest' for the object version. This does not cause a render but the
    // component's variables for the URL parameters do become out of sync with the URL. We do this so that if the user shares the
    // link then the link is for the version they are looking at and not the 'latest' which could change at any point.
    useEffect(() => {

        let shouldRewriteUrl = true

        if (!isDownloading && tag && tag.header?.objectId === objectId && (objectVersion === "latest" || tagVersion === "latest")) {
            if (shouldRewriteUrl && tag?.header?.objectVersion) {
                rewriteUrlOfObjectParam(tag.header.objectVersion)
            }
        }

        return () => {
            shouldRewriteUrl = false
        }

    }, [isDownloading, objectId, objectVersion, tag, tagVersion])

    /**
     * A function that toggles showing the info modal for the full job metadata.
     */
    const toggleJobInfoModal = useCallback(() => {
        setShowJobModal(showJobModal => !showJobModal)
    }, [])

    return (

        <React.Fragment>

            {!props.tagSelector && urlDetails?.errorMessages && urlDetails?.errorMessages.length > 0 &&
                <Alert className={"mt-4 mb-4"} variant={"danger"} showBullets={true} listHasHeader={true}>
                    {["Downloading the job information failed due to the URL parameters being invalid "].concat(urlDetails?.errorMessages ?? [])}
                </Alert>
            }

            {isDownloading &&
                <Loading text={"Please wait..."}/>
            }

            {!isDownloading && tag && (props.tagSelector || urlDetails?.errorMessages.length === 0) &&

                <React.Fragment>

                    <TextBlock>
                        The buttons below can be used to download the JSON and PDF versions of the job information shown
                        below.
                    </TextBlock>

                    <IsLatestVersion className={"mb-4"} showMessageWhenIsLatest={false} tagSelector={tag?.header}/>

                    {tag?.attrs?.trac_job_status?.stringValue &&
                        <Alert className={"mb-4"} variant={jobStatusLookup[tag.attrs.trac_job_status.stringValue].variant}>
                            {`${jobStatusLookup[tag.attrs.trac_job_status.stringValue].message}${tag.attrs.hasOwnProperty("trac_job_error_message") ? `. ${tag.attrs.trac_job_error_message?.stringValue}.` : ""}`}
                        </Alert>
                    }

                    <ButtonToDownloadJson className={"me-5"}
                                          loading={isDownloading}
                                          tag={tag}
                    />

                    <ButtonToDownloadPdf className={"me-5"}
                                         loading={isDownloading}
                                         tag={tag}
                    />

                    <ButtonToEmailObjectLink loading={isDownloading}
                                             tagSelector={tag.header}
                    />

                    <HeaderTitle type={"h3"} text={"Job details"}>
                        <Button ariaLabel={"Show job info"}
                                className={"min-width-px-150"}
                                isDispatched={false}
                                onClick={toggleJobInfoModal}
                                variant={"outline-info"}
                        >
                            View full info
                        </Button>
                    </HeaderTitle>

                    <ObjectDetails bordered={false}
                                   className={"pb-3"}
                                   metadata={tag}
                                   striped={true}
                    />

                    <MetadataViewerModal show={showJobModal}
                                         tag={tag}
                                         title={"Full job metadata"}
                                         toggle={toggleJobInfoModal}
                    />

                    {tag.definition?.job?.runFlow &&
                        <JobViewerRunFlow job={tag}/>
                    }
                    {tag.definition?.job?.runModel &&
                        <JobViewerRunModel job={tag}/>
                    }
                    {tag.definition?.job?.importModel &&
                        <JobViewerImportModel job={tag}/>
                    }

                </React.Fragment>
            }

        </React.Fragment>
    )
};

JobViewer.propTypes = {

    getTagFromUrl: PropTypes.bool.isRequired,
    tagSelector: PropTypes.object
};