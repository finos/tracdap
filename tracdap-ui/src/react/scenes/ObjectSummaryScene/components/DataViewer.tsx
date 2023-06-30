/**
 * A component that shows a summary of a particular version of a dataset stored in TRAC. This is used by both
 * the ObjectSummaryScene and the DataInfoModal. This component is for viewing in a browser, there is a sister
 * component called {@link DataViewerPdf} that is for viewing in a PDF. These two components need to be kept
 * in sync so if a change is made to one then it should be reflected in the other.
 *
 * @remarks
 * This component can either be passed a tag selector as a prop or it can get the object details from the URL
 * and use that to form a tag selector.
 *
 * @module DataViewer
 * @category ObjectSummaryScene Component
 */

import {Alert} from "../../../components/Alert";
import {ButtonToDownloadJson} from "../../../components/ButtonToDownloadJson";
import {ButtonToDownloadPdf} from "../../../components/ButtonToDownloadPdf";
import {ButtonToDownloadData} from "../../../components/ButtonToDownloadData";
import {ButtonToEmailObjectLink} from "../../../components/ButtonToEmailObjectLink";
import {IsLatestVersion} from "../../../components/IsLatestVersion";
import {Loading} from "../../../components/Loading";
import {getTagSelectorFromUrlParameters} from "../../../utils/utils_trac_metadata";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {MetadataViewer} from "../../../components/MetadataViewer/MetadataViewer";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import PropTypes from "prop-types";
import React, {useEffect} from "react";
import {rewriteUrlOfObjectParam} from "../../../utils/utils_general";
import {SchemaFieldsTable} from "../../../components/SchemaFieldsTable/SchemaFieldsTable";
import {ShowHideDetails} from "../../../components/ShowHideDetails";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useMetadataStore} from "../../../utils/utils_async";
import {useAppSelector} from "../../../../types/types_hooks";
import {useNavigate, useParams} from "react-router-dom";

/**
 * An interface for the props of the DataViewer component.
 */
export interface Props {

    /**
     * Whether to look at the URL parameters to define the metadata to fetch.
     */
    getTagFromUrl: boolean
    /**
     * The tag selector for the data to show a summary of.
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

export const DataViewer = (props: Props) => {

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
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.DATA].to}/${objectId}/latest/latest`)
            }

        } else if (getTagFromUrl && props.tagSelector && objectId === undefined && objectVersion === undefined && tagVersion === undefined) {

            // If we were provided with a tag we still want to update the URL to match the requested tag, but we need to avoid an infinite loop
            // so only do this when there are no URL parameters. If we do this when the parameters are set we continuously trigger re-renders that
            // cause the item to be re-fetched
            if (shouldNavigate) {
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.DATA].to}/${props.tagSelector.objectId}/${props.tagSelector.latestObject ? "latest" : props.tagSelector.objectVersion}/${props.tagSelector.latestTag ? "latest" : props.tagSelector.tagVersion}`)
            }
        }

        // Do not allow redirects if this component unmounts
        return () => {
            shouldNavigate = false
        }

    }, [getTagFromUrl, navigate, objectId, objectVersion, props.tagSelector, tagVersion])

    const urlDetails = !getTagFromUrl ? undefined : getTagSelectorFromUrlParameters({objectType: trac.ObjectType.DATA, objectId, objectVersion, tagVersion, searchAsOf})

    // If we receive a prop with a tag selector then user that, otherwise use the URL parameters, but only if there are no errors
    const tagSelector = props.tagSelector ? props.tagSelector : urlDetails && urlDetails?.errorMessages.length === 0 ? urlDetails?.tagSelector : undefined

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting a Tag for an object in TRAC along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There is a callback function that is passed which runs whenever the tag is
    // updated
    const [isDatasetTagDownloading, datasetTag, status] = useMetadataStore(tagSelector, {suppressError: true})

    if (urlDetails !== undefined && status === "failed") {
        urlDetails.errorMessages.push("That dataset object ID and/or version numbers do not exist.")
    }

    // If the dataset is created using a schema object rather than having a schema fields assigned when loaded, then we get the schema
    // metadata too in order to provide the schema.
    const [, schemaTag] = useMetadataStore(datasetTag?.definition?.data?.schemaId)

    // A hook  that updates the URL in the browser to remove 'latest' for the object or tag version. This does not cause a render but the
    // component's variables for the URL parameters do become out of sync with the URL. We do this so that if the user shares the
    // link then the link is for the version they are looking at and not the 'latest' which could change at any point.
    useEffect(() => {

        let shouldRewriteUrl = true

        if (!isDatasetTagDownloading && datasetTag && datasetTag.header?.objectId === objectId && (objectVersion === "latest" || tagVersion === "latest")) {
            if (shouldRewriteUrl && datasetTag?.header?.objectVersion) {
               rewriteUrlOfObjectParam(datasetTag.header.objectVersion)
            }
        }

        return () => {
            shouldRewriteUrl = false
        }

    }, [isDatasetTagDownloading, objectId, objectVersion, datasetTag, tagVersion])

    return (

        <React.Fragment>

            {/*If the user is visiting by clicking on a link to view the job we have to check that the parameters are valid*/}
            {!props.tagSelector && urlDetails?.errorMessages && urlDetails?.errorMessages.length > 0 &&
                <Alert className={"mt-4 mb-4"} variant={"danger"} showBullets={true} listHasHeader={true}>
                    {["Downloading the dataset information failed due to the URL parameters being invalid "].concat(urlDetails?.errorMessages ?? [])}
                </Alert>
            }

            {isDatasetTagDownloading &&
                <Loading text={"Please wait..."}/>
            }

            {!isDatasetTagDownloading && datasetTag && (props.tagSelector || urlDetails?.errorMessages.length === 0) &&

                <React.Fragment>

                    <TextBlock>
                        The buttons below can be used to download the JSON and PDF versions of the dataset information shown
                        below. Clicking on the CSV button will download the data itself. Note that number of rows that can be
                        downloaded is limited and downloads of very large datasets can fail.
                    </TextBlock>

                    <IsLatestVersion className={"mb-4"} showMessageWhenIsLatest={false} tagSelector={datasetTag?.header}/>

                    <ButtonToDownloadJson className={"me-5"}
                                          loading={isDatasetTagDownloading}
                                          tag={datasetTag}
                    />

                    <ButtonToDownloadPdf className={"me-5"}
                                         loading={isDatasetTagDownloading}
                                         tag={datasetTag}
                    />

                    <ButtonToDownloadData className={"me-5"}
                                          loading={isDatasetTagDownloading}
                                          tag={datasetTag}
                    />

                    <ButtonToEmailObjectLink loading={isDatasetTagDownloading}
                                             tagSelector={datasetTag.header}
                    />

                    <HeaderTitle outerClassName={"mt-4 mb-2"} type={"h4"} text={"Metadata"}/>

                    <MetadataViewer metadata={datasetTag} className={"pb-2"}/>

                    {/*Datasets can be defined with their own schema or with a schema object, this is with their own schema*/}
                    {!datasetTag.definition?.data?.schemaId && datasetTag.definition?.data?.schema?.table?.fields &&
                        <SchemaFieldsTable className={"mt-5"}
                                           fields={datasetTag.definition?.data?.schema?.table?.fields}
                        >
                            {/*This places the header and the search on the same row*/}
                            <h4>Schema</h4>
                        </SchemaFieldsTable>
                    }

                    {/*Datasets can be defined with their own schema or with a schema object, this is with a schema object*/}
                    {datasetTag.definition?.data?.schemaId && schemaTag?.definition?.schema?.table?.fields &&
                        <React.Fragment>
                            <HeaderTitle type={"h4"} text={"Schema"}/>

                            <TextBlock>
                                This dataset uses a schema object to define its fields:
                            </TextBlock>

                            <ShowHideDetails classNameOuter={"pt-2 pb-4"} classNameInner={"pt-1 pb-0"} linkText={"metadata"}>
                                <MetadataViewer className={"py-0"}
                                                metadata={schemaTag}/>
                            </ShowHideDetails>

                            <SchemaFieldsTable fields={schemaTag?.definition?.schema?.table?.fields}/>
                        </React.Fragment>
                    }

                    {/*TODO Add deltas and parts viewer*/}

                </React.Fragment>
            }
        </React.Fragment>
    )
};

DataViewer.propTypes = {

    getTagFromUrl: PropTypes.bool.isRequired,
    tagSelector: PropTypes.object
};