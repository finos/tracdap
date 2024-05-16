/**
 * A component that shows a summary of a particular version of a flow stored in TRAC. This is used by both
 * the ObjectSummaryScene and the FlowInfoModal. This component is for viewing in a browser, there is a sister
 * component called {@link FlowViewerPdf} that is for viewing in a PDF. These two components need to be kept
 * in sync so if a change is made to one then it should be reflected in the other.
 *
 * @remarks
 * This component can either be passed a tag selector as a prop or it can get the object details from the URL
 * and use that to form a tag selector.
 *
 * @module FLowViewer
 * @category ObjectSummaryScene component
 */

import {Alert} from "../../../components/Alert";
import {ButtonToDownloadJson} from "../../../components/ButtonToDownloadJson";
import {ButtonToDownloadPdf} from "../../../components/ButtonToDownloadPdf";
import {ButtonToEmailObjectLink} from "../../../components/ButtonToEmailObjectLink";
import {FlowKeyForPdf} from "../../../components/PdfReport/FlowKeyForPdf";
import FlowSvg from "./FlowSvg";
import {FlowSvgForPdf} from "../../../components/PdfReport/FlowSvgForPdf";
import {getTagSelectorFromUrlParameters} from "../../../utils/utils_trac_metadata";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {IsLatestVersion} from "../../../components/IsLatestVersion";
import {Loading} from "../../../components/Loading";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import {MetadataViewer} from "../../../components/MetadataViewer/MetadataViewer";
import {ObjectSummaryStoreState, setFlowNodeOptions} from "../store/objectSummaryStore";
import PropTypes from "prop-types";
import React, {Suspense, useEffect} from "react";
import {rewriteUrlOfObjectParam} from "../../../utils/utils_general";
import SelectedNodeInfo from "./SelectedNodeInfo";
import SelectFlowNode from "./SelectFlowNode";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {useMetadataStore} from "../../../utils/utils_async";
import {useNavigate, useParams} from "react-router-dom";

/**
 * An interface for the props of the FlowViewer component.
 */
export interface Props {

    /**
     * Whether to look at the URL parameters to define the metadata to fetch.
     */
    getTagFromUrl: boolean
    /**
     * The key in the objectSummaryStore to get the state for the flow visualisation.
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
    /**
     * The tag selector for the flow to show a summary of.
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

export const FlowViewer = (props: Props) => {

    const {getTagFromUrl, storeKey} = props

    // Get what we need from the store
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)
    const {pdfChartProps} = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].pdf)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

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
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.FLOW].to}/${objectId}/latest/latest`)
            }

        } else if (getTagFromUrl && props.tagSelector && objectId === undefined && objectVersion === undefined && tagVersion === undefined) {

            // If we were provided with a tag we still want to update the URL to match the requested tag, but we need to avoid an infinite loop
            // so only do this when there are no URL parameters. If we do this when the parameters are set we continuously trigger re-renders that
            // cause the item to be re-fetched
            if (shouldNavigate) {
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.FLOW].to}/${props.tagSelector.objectId}/${props.tagSelector.latestObject ? "latest" : props.tagSelector.objectVersion}/${props.tagSelector.latestTag ? "latest" : props.tagSelector.tagVersion}`)
            }
        }

        // Do not allow redirects if this component unmounts
        return () => {
            shouldNavigate = false
        }
    }, [getTagFromUrl, navigate, objectId, objectVersion, props.tagSelector, tagVersion])

    const urlDetails = !getTagFromUrl ? undefined : getTagSelectorFromUrlParameters({objectType: trac.ObjectType.FLOW, objectId, objectVersion, tagVersion, searchAsOf})

    // If we receive a prop with a tag selector then user that, otherwise use the URL parameters, but only if there are no errors
    const tagSelector = props.tagSelector ? props.tagSelector : urlDetails && urlDetails?.errorMessages.length === 0 ? urlDetails?.tagSelector : undefined

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting a Tag for an object in TRAC along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There is a callback function that is passed which runs whenever the tag is
    // updated
    const [isDownloading, tag, status] = useMetadataStore(tagSelector, {
        callback: (tag: null | trac.metadata.ITag) => tag != null && dispatch(setFlowNodeOptions({tag, storeKey})),
        suppressError: true
    })

    if (urlDetails !== undefined && status === "failed") {
        urlDetails.errorMessages.push("That flow object ID and/or version numbers do not exist.")
    }

    // A hook  that updates the URL in the browser to remove 'latest' for the object or tag version. This does not cause a render but the
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

    return (

        <React.Fragment>

            {/*If the user is visiting by clicking on a link to view the job we have to check that the parameters are valid*/}
            {!props.tagSelector && urlDetails?.errorMessages && urlDetails?.errorMessages.length > 0 &&
                <Alert className={"mt-4 mb-4"} variant={"danger"} showBullets={true} listHasHeader={true}>
                    {["Downloading the flow information failed due to the URL parameters being invalid "].concat(urlDetails?.errorMessages ?? [])}
                </Alert>
            }

            {isDownloading &&
                <Loading text={"Please wait..."}/>
            }

            {!isDownloading && tag && (props.tagSelector || urlDetails?.errorMessages.length === 0) &&

                <React.Fragment>

                    <TextBlock>
                        The buttons below can be used to download the JSON and PDF versions of the flow information shown
                        below.
                    </TextBlock>

                    <IsLatestVersion className={"mb-4"} showMessageWhenIsLatest={false} tagSelector={tag?.header}/>

                    <ButtonToDownloadJson className={"me-5"}
                                          loading={isDownloading}
                                          tag={tag}
                    />

                    {/*In the store we have a single object that has all the props that the*/}
                    {/*SVG flow components need*/}
                    <ButtonToDownloadPdf className={"me-5"}
                                         loading={isDownloading}
                                         svg={pdfChartProps && <FlowSvgForPdf {...pdfChartProps}/>}
                                         svgKey={pdfChartProps && <FlowKeyForPdf {...pdfChartProps}/>}
                                         tag={tag}
                    />

                    <ButtonToEmailObjectLink loading={isDownloading}
                                             tagSelector={tag.header}
                    />

                    <HeaderTitle outerClassName={"mt-4 mb-2"} type={"h4"} text={"Metadata"}/>

                    <MetadataViewer metadata={tag} className={"pb-2"}/>

                    <HeaderTitle type={"h4"} text={"Visualisation"}/>

                    <TextBlock className={"my-2"}>
                        A visualisation of the flow is shown below, clicking on a model or dataset will bring up
                        information about that item. Alternatively the select box below can be used to find the
                        item you are interested in. Note that the flow does not contain information about the expected
                        schemas of the input and output datasets.
                    </TextBlock>

                    <Suspense fallback={<Loading/>}>

                        <SelectFlowNode storeKey={storeKey}/>
                        <FlowSvg flow={tag} storeKey={storeKey}/>
                        <SelectedNodeInfo storeKey={storeKey}/>

                    </Suspense>

                </React.Fragment>
            }

        </React.Fragment>
    )
};

FlowViewer.propTypes = {

    tagSelector: PropTypes.object,
};