/**
 * A component that shows a summary of a particular version of a model stored in TRAC. This is used by both
 * the ObjectSummaryScene and the ModelInfoModal. This component is for viewing in a browser, there is a sister
 * component called {@link ModelViewerPdf} that is for viewing in a PDF. These two components need to be kept
 * in sync so if a change is made to one then it should be reflected in the other.
 *
 * @remarks
 * This component can either be passed a tag selector as a prop or it can get the object details from the URL
 * and use that to form a tag selector.
 *
 * @module ModelViewer
 * @category ObjectSummaryScene Component
 */

import {Alert} from "../../../components/Alert";
import {AttachedFileTable} from "../../../components/AttachedFileTable";
import {ButtonToDownloadJson} from "../../../components/ButtonToDownloadJson";
import {ButtonToDownloadPdf} from "../../../components/ButtonToDownloadPdf";
import {ButtonToViewCode} from "../../../components/ButtonToViewCode";
import {convertArrayToOptions} from "../../../utils/utils_arrays";
import {getTagSelectorFromUrlParameters} from "../../../utils/utils_trac_metadata";
import {matchModelToRepositoryConfigAndGetUrl, rewriteUrlOfObjectParam} from "../../../utils/utils_general";
import {ModelDatasetListTable} from "../../../components/ModelDatasetListTable";
import {hasOwnProperty} from "../../../utils/utils_trac_type_chckers";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {IsLatestVersion} from "../../../components/IsLatestVersion";
import {Loading} from "../../../components/Loading";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import {MetadataViewer} from "../../../components/MetadataViewer/MetadataViewer";
import {Option, SelectOptionPayload} from "../../../../types/types_general";
import {ParameterTable} from "../../../components/ParameterTable";
import React, {useCallback, useEffect, useState} from "react";
import {SchemaFieldsTable} from "../../../components/SchemaFieldsTable/SchemaFieldsTable";
import {SelectOption} from "../../../components/SelectOption";
import {ShowHideDetails} from "../../../components/ShowHideDetails";
import {SingleValue} from "react-select";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";
import {useMetadataStore} from "../../../utils/utils_async";
import {useNavigate, useParams} from "react-router-dom";
import PropTypes from "prop-types";
import {ButtonToEmailObjectLink} from "../../../components/ButtonToEmailObjectLink";

/**
 * An interface for the props of the ModelViewer component.
 */
export interface Props {

    /**
     * Whether to look at the URL parameters to define the metadata to fetch.
     */
    getTagFromUrl: boolean
    /**
     * The tag selector for the model to show a summary of. If not set the component will look for parameters in the URL
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

/**
 * An interface for the state of the ModelViewer component.
 */
export interface State {

    /**
     * The input dataset options for the schema to view .
     */
    inputSchemaOptions: Option<string>[]
    /**
     * The output dataset options for the schema to view .
     */
    outputSchemaOptions: Option<string>[]
    /**
     * The selected input dataset for the model to view the schema for.
     */
    selectedInputSchemaOption: SingleValue<Option<string>>
    /**
     * The selected output dataset for the model to view the schema for.
     */
    selectedOutputSchemaOption: SingleValue<Option<string>>
}

/**
 * The initial state for the component.
 */
const initialState = {

    inputSchemaOptions: [],
    outputSchemaOptions: [],
    selectedInputSchemaOption: null,
    selectedOutputSchemaOption: null
}

export const ModelViewer = (props: Props) => {

    const {getTagFromUrl} = props

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {codeRepositories} = useAppSelector(state => state["applicationStore"].clientConfig)
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
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.MODEL].to}/${objectId}/latest/latest`)
            }

        } else if (getTagFromUrl && props.tagSelector && objectId === undefined && objectVersion === undefined && tagVersion === undefined) {

            // If we were provided with a tag we still want to update the URL to match the requested tag, but we need to avoid an infinite loop
            // so only do this when there are no URL parameters. If we do this when the parameters are set we continuously trigger re-renders that
            // cause the item to be re-fetched
            if (shouldNavigate) {
                navigate(`${ObjectSummaryPaths[trac.metadata.ObjectType.MODEL].to}/${props.tagSelector.objectId}/${props.tagSelector.latestObject ? "latest" : props.tagSelector.objectVersion}/${props.tagSelector.latestTag ? "latest" : props.tagSelector.tagVersion}`)
            }
        }

        // Do not allow redirects if this component unmounts
        return () => {
            shouldNavigate = false
        }

    }, [getTagFromUrl, navigate, objectId, objectVersion, props.tagSelector, tagVersion])

    // Additional state holding information about the options that the user has set
    const [{
        inputSchemaOptions,
        outputSchemaOptions,
        selectedInputSchemaOption,
        selectedOutputSchemaOption,
    }, setState] = useState<State>(initialState)

    // TODO Add lineage component at the bottom.

    /**
     * A function that runs when the user clicks to select a different schema to view.
     * @param payload - The returned option from the SelectOption component.
     */
    const handleSchemaSelectionChange = useCallback((payload: SelectOptionPayload<Option<string>, false>) => {

        const {id, value} = payload
        if (typeof id === "string") setState(prevState => ({...prevState, [id]: value}))

    }, [])

    const urlDetails = !getTagFromUrl ? undefined : getTagSelectorFromUrlParameters({objectType: trac.ObjectType.MODEL, objectId, objectVersion, tagVersion, searchAsOf})

    // If we receive a prop with a tag selector then user that, otherwise use the URL parameters, but only if there are no errors
    const tagSelector = props.tagSelector ? props.tagSelector : urlDetails && urlDetails?.errorMessages.length === 0 ? urlDetails?.tagSelector : undefined

    /**
     * A function that is used as a callback to the {@link useMetadataStore} hook which is run after the metadata is downloaded. In this case it created options for the
     * input and output schemas and sets some initial selections.
     */
    const getTagCallback = useCallback((tag: null | trac.metadata.ITag) => {

        // Set the schema options =
        const inputSchemaOptions = convertArrayToOptions(Object.keys(tag?.definition?.model?.inputs || {}), false)
        const outputSchemaOptions = convertArrayToOptions(Object.keys(tag?.definition?.model?.outputs || {}), false)

        // Set a default selected schema for the SchemaFieldsTable component
        const selectedInputSchemaOption = inputSchemaOptions.length > 0 ? inputSchemaOptions[0] : null
        const selectedOutputSchemaOption = outputSchemaOptions.length > 0 ? outputSchemaOptions[0] : null

        // Update state
        setState(prevState => ({
            ...prevState,
            inputSchemaOptions,
            outputSchemaOptions,
            selectedInputSchemaOption,
            selectedOutputSchemaOption
        }))

    }, [])

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting a Tag for an object in TRAC along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There is a callback function that is passed which runs whenever the tag is
    // updated
    const [isDownloading, tag, status] = useMetadataStore(tagSelector, {callback: getTagCallback, suppressError: true})

    if (urlDetails !== undefined && status === "failed") {
        urlDetails.errorMessages.push("That model object ID and/or version numbers do not exist.")
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

    const selectedInputSchema = selectedInputSchemaOption && tag?.definition?.model?.inputs && hasOwnProperty(tag.definition.model.inputs, selectedInputSchemaOption.value) ? tag.definition?.model?.inputs[selectedInputSchemaOption.value].schema?.table?.fields : null
    const selectedOutputSchema = selectedOutputSchemaOption && tag?.definition?.model?.outputs && hasOwnProperty(tag.definition.model.outputs, selectedOutputSchemaOption.value) ? tag.definition?.model?.outputs[selectedOutputSchemaOption.value].schema?.table?.fields : null

    return (

        <React.Fragment>

            {/*If the user is visiting by clicking on a link to view the job we have to check that the parameters are valid*/}
            {!props.tagSelector && urlDetails?.errorMessages && urlDetails?.errorMessages.length > 0 &&
                <Alert className={"mt-4 mb-4"} variant={"danger"} showBullets={true} listHasHeader={true}>
                    {["Downloading the model information failed due to the URL parameters being invalid "].concat(urlDetails?.errorMessages ?? [])}
                </Alert>
            }

            {isDownloading &&
                <Loading text={"Please wait..."}/>
            }

            {!isDownloading && tag && (props.tagSelector || urlDetails?.errorMessages.length === 0) &&
                <React.Fragment>
                    <TextBlock>
                        The buttons below can be used to download the JSON and PDF versions of the model definition
                        shown below. Clicking on the code file button will take you to the code in the model
                        repository, you will need an account to access this site.
                    </TextBlock>

                    {tenant !== undefined && matchModelToRepositoryConfigAndGetUrl(codeRepositories, tag, tenant) === undefined &&
                        <Alert className={"mb-4"} variant={"warning"} showBullets={false}>
                            You are unable to view the source code for this model. This is usually because the application
                            doesn&apos;t have a definition for this model&apos;s repository ({tag?.attrs?.trac_model_repository?.stringValue || "unknown"}) so
                            it doesn&apos;t know the location of the file or the source code is unavailable to view, for example it was loaded from a python package.
                        </Alert>
                    }

                    <IsLatestVersion className={"mb-4"} showMessageWhenIsLatest={false} tagSelector={tag?.header}/>

                    <ButtonToDownloadJson className={"me-5"}
                                          loading={isDownloading}
                                          tag={tag}
                    />

                    <ButtonToDownloadPdf className={"me-5"}
                                         loading={isDownloading}
                                         tag={tag}
                    />

                    <ButtonToViewCode className={"me-5"}
                                      loading={isDownloading}
                                      tag={tag}
                    />

                    <ButtonToEmailObjectLink loading={isDownloading}
                                             tagSelector={tag.header}
                    />

                    <HeaderTitle outerClassName={"mt-4 mb-2"} type={"h4"} text={"Metadata"}/>

                    <MetadataViewer metadata={tag} className={"pb-2"}/>

                    <HeaderTitle type={"h4"} text={"Attached documents"}/>

                    {/*<ButtonToAttachDocument/>*/}

                    <AttachedFileTable className={"mb-4"}
                                       tagSelector={tag.header}
                    />

                    <HeaderTitle type={"h4"} text={"Parameters"}/>

                    <ParameterTable className={"mb-4"}
                                    params={tag.definition?.model?.parameters}
                    />

                    <HeaderTitle type={"h4"} text={"Input datasets"}/>

                    <ModelDatasetListTable className={""}
                                           datasets={tag.definition?.model?.inputs}
                                           datasetType={"input"}
                    />
                    <ShowHideDetails classNameOuter={"mt-2 pb-3"}
                                     classNameInner={"mt-4 mb-0"}
                                     iconType={"arrow"}
                                     linkText={"input schemas"}>

                        {selectedInputSchema ?
                            <SchemaFieldsTable fields={selectedInputSchema}>

                                <SelectOption basicType={trac.STRING}
                                              className={"pe-2 me-2 pe-lg-4 me-lg-4"}
                                              id={"selectedInputSchemaOption"}
                                              isDispatched={false}
                                              labelPosition={"left"}
                                              labelText={"Select an input:"}
                                              mustValidate={false}
                                              onChange={handleSchemaSelectionChange}
                                              options={inputSchemaOptions}
                                              validateOnMount={false}
                                              value={selectedInputSchemaOption}
                                              showValidationMessage={false}
                                />

                            </SchemaFieldsTable> : null}
                    </ShowHideDetails>

                    <HeaderTitle type={"h4"} text={"Output datasets"}/>

                    <ModelDatasetListTable datasets={tag.definition?.model?.outputs} datasetType={"output"}/>

                    <ShowHideDetails classNameOuter={"mt-2 pb-3"}
                                     classNameInner={"mt-4 mb-0"}
                                     iconType={"arrow"}
                                     linkText={"output schemas"}>

                        {selectedOutputSchema ?
                            <SchemaFieldsTable fields={selectedOutputSchema}>

                                <SelectOption basicType={trac.STRING}
                                              className={"pe-2 me-2 pe-lg-4 me-lg-4"}
                                              id={"selectedOutputSchemaOption"}
                                              isDispatched={false}
                                              labelPosition={"left"}
                                              labelText={"Select an output:"}
                                              mustValidate={false}
                                              onChange={handleSchemaSelectionChange}
                                              options={outputSchemaOptions}
                                              validateOnMount={false}
                                              value={selectedOutputSchemaOption}
                                              showValidationMessage={false}
                                />

                            </SchemaFieldsTable> : null}

                    </ShowHideDetails>
                </React.Fragment>
            }

        </React.Fragment>
    )
};

ModelViewer.propTypes = {

    getTagFromUrl: PropTypes.bool.isRequired,
    tagSelector: PropTypes.object
};
