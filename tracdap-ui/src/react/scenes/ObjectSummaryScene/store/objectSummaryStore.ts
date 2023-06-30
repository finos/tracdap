import {
    convertFlowNodesToOptions,
    getDatasetFromFlow,
    getModelInputsFromFlow,
    getModelOutputsFromFlow,
    getOptionFromNodeId,
    getRenamedDatasets,
    getSearchQueryFromFlowNode
} from "../../../utils/utils_flows";
import {convertObjectTypeToString, createUniqueObjectKey} from "../../../utils/utils_trac_metadata";
import {createSlice, PayloadAction} from '@reduxjs/toolkit';
import {FlowGroup, GraphLayout, KeyLabels, LabelSizes, NodeDetails, Option, RenamedDataset, StoreStatus} from "../../../../types/types_general";
import {isKeyOf} from "../../../utils/utils_trac_type_chckers";
import {SingleValue} from "react-select";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Value} from 'react-svg-pan-zoom';

/**
 * A type for the props needed to render the PDF versions of the SVG and the key. These are used in the {@link FlowSvgForPdf}
 * and {@link FlowKeyForPdf} components and need to be saved from the browser versions of these components.
 */

type PdfChartProps = {
    graphLayout: GraphLayout
    arrowLength: number
    fontSize: number
    labelSizes: LabelSizes
    // Needed for the SVG key
    boxXPadding: number
    boxYPadding: number
    keyLabels: KeyLabels
}

/**
 * A type for the additional properties for each visualisation of a flow in the {@link FlowViewer} component. By
 * storing these in a store rather than in state we can retain the user's selections across the pages.
 */
type FlowUser = {

    objectKey: null | string
    chart: {
        flowDirection: "DOWN" | "RIGHT"
        showRenames: boolean
        selectedNodeId: null | string
        selectedTool: { real: "none" | "pan" | "zoom-in" | "zoom-out", override: "none" | "pan" | "zoom-in" | "zoom-out" }
    },
    nodes: {
        definition: trac.metadata.IFlowDefinition
        optionsWithRenames: FlowGroup
        optionsWithoutRenames: FlowGroup
        renamedDatasets: RenamedDataset[]
        selectedDataset: { [key: string]: trac.metadata.IFlowNode }
        selectedNodeOption: SingleValue<Option<string, NodeDetails>>
        selectedModelInputs: { [key: string]: trac.metadata.IFlowNode }
        selectedModelOutputs: { [key: string]: trac.metadata.IFlowNode & { type: "output" | "intermediate" } }
        selectedNodeSearchQuery: { [key: string]: undefined | { attrName: string, value: string } }
    }
    pdf: {
        pdfChartProps: null | PdfChartProps
    },
    svgParametersLoadedFromComponent: boolean,
    svg: { settings: Value }
}

/**
 * A function that returns an initial state for a new user of the store when viewing a flow.
 */
function addFlowUser(): FlowUser {

    return {
        objectKey: null,
        chart: {
            flowDirection: "RIGHT",
            showRenames: false,
            selectedNodeId: null,
            selectedTool: {real: "none", override: "none"}
        },
        nodes: {
            definition: {},
            optionsWithRenames: [
                {label: "Models", options: []},
                {label: "Inputs", options: []},
                {label: "Outputs", options: []},
                {label: "Intermediates", options: []}
            ],
            optionsWithoutRenames: [
                {label: "Models", options: []},
                {label: "Inputs", options: []},
                {label: "Outputs", options: []},
                {label: "Intermediates", options: []}
            ],
            renamedDatasets: [],
            selectedDataset: {},
            selectedNodeOption: null,
            selectedModelInputs: {},
            selectedModelOutputs: {},
            selectedNodeSearchQuery: {}
        },
        pdf: {pdfChartProps: null},
        svgParametersLoadedFromComponent: false,
        svg: {
            settings: {
                version: 2,
                mode: 'idle',
                focus: false,
                a: 1,
                b: 0,
                c: 0,
                d: 0,
                e: 0,
                f: 0,
                viewerWidth: 1,
                viewerHeight: 1,
                SVGWidth: 1,
                SVGHeight: 1,
                startX: undefined,
                startY: undefined,
                endX: undefined,
                endY: undefined,
                miniatureOpen: false
            }
        }
    }
}

// Define a type for the slice state
export interface ObjectSummaryStoreState {

    // The status of any API requests to get tag for the store
    status: StoreStatus
    // Any message associated with getting a tag via the API
    message: undefined | string
    // The object tagSelector and tag by object type
    metadata: {
        DATA: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        MODEL: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        SCHEMA: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        JOB: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        FLOW: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        },
        CUSTOM: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        STORAGE: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        FILE: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
        OBJECT_TYPE_NOT_SET: {
            tagSelector: undefined | trac.metadata.ITagSelector
            tag: undefined | trac.metadata.ITag
        }
    }
    // We store one set of information about the flow being viewed for each use case. If the flow object is
    // changed the previous view is removed
    flow: {
        jobViewerRunFlow: FlowUser
        runAFlow: FlowUser
        searchResult: FlowUser
    }
}

// This is the initial state of the store.
const initialState: ObjectSummaryStoreState = {
    status: "idle",
    message: undefined,
    metadata: {
        DATA: {
            tagSelector: undefined,
            tag: undefined
        },
        MODEL: {
            tagSelector: undefined,
            tag: undefined
        },
        SCHEMA: {
            tagSelector: undefined,
            tag: undefined
        },
        JOB: {
            tagSelector: undefined,
            tag: undefined
        },
        FLOW: {
            tagSelector: undefined,
            tag: undefined
        },
        STORAGE: {
            tagSelector: undefined,
            tag: undefined
        },
        CUSTOM: {
            tagSelector: undefined,
            tag: undefined
        },
        FILE: {
            tagSelector: undefined,
            tag: undefined
        },
        OBJECT_TYPE_NOT_SET: {
            tagSelector: undefined,
            tag: undefined
        }
    },
    flow: {
        jobViewerRunFlow: addFlowUser(),
        runAFlow: addFlowUser(),
        searchResult: addFlowUser()
    }
}

export const objectSummaryStore = createSlice({
    name: 'objectSummaryStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its original state.
         */
        resetState: () => initialState,
        /**
         * A reducer that runs when the user wants to view an object in one of the object viewers, it adds the tagSelector
         * for the object into the right part of the store which will trigger the application to get the tag for it.
         *
         * Note that the viewing components e.g. {@link DataViewer} were updated to be able to handle URL parameters so
         * that they can download the metadata tag themselves, this means that the metadata does not need to be kept
         * in a store. This store was used prior to this update but now this reducer is not used, it is kept in case it
         * is needed in the future.
         */
        setViewTagSelector: (state, action: PayloadAction<trac.metadata.ITagSelector>) => {

            const {objectType} = action.payload

            if (objectType) {

                const objectTypeAString = convertObjectTypeToString(objectType, false, false)
                if (isKeyOf(state.metadata, objectTypeAString)) state.metadata[objectTypeAString].tagSelector = action.payload
            }
        },
        /**
         * A reducer that runs when the user clicks on a box in the flow SVG, this stores the ID of the selected node.
         */
        setSelectedNodeId: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], selectedNodeId: null | string }>) => {

            // Store the key of the selected node
            const {payload: {storeKey, selectedNodeId}} = action

            // Store the key of the selected node
            state.flow[storeKey].chart.selectedNodeId = selectedNodeId

            // Store the selected option
            state.flow[storeKey].nodes.selectedNodeOption = selectedNodeId ? getOptionFromNodeId(selectedNodeId, state.flow[storeKey].nodes.optionsWithRenames) || null : null

            // If the selected node is not null then get all the information needed to show the user
            if (selectedNodeId !== null) {

                // The first part of the key is the flow node type integer
                const type = selectedNodeId ? parseInt(selectedNodeId.split("-")[0]) : null

                // The second part of the key is the key used in the flow
                const key = type ? selectedNodeId.substring(1 + selectedNodeId.indexOf("-")) : null

                // Get the information for when the user has selected a model
                if (key && type === trac.FlowNodeType.MODEL_NODE) {

                    state.flow[storeKey].nodes.selectedModelInputs = getModelInputsFromFlow(state.flow[storeKey].nodes.definition, key, state.flow[storeKey].nodes.renamedDatasets)
                    state.flow[storeKey].nodes.selectedModelOutputs = getModelOutputsFromFlow(state.flow[storeKey].nodes.definition, key)
                    state.flow[storeKey].nodes.selectedNodeSearchQuery = getSearchQueryFromFlowNode(state.flow[storeKey].nodes.definition, key)

                    // Blank the dataset only properties
                    state.flow[storeKey].nodes.selectedDataset = {}

                } else if (key && (type === trac.FlowNodeType.INPUT_NODE || type === trac.FlowNodeType.OUTPUT_NODE)) {

                    // Get the information for when the user has selected a dataset
                    state.flow[storeKey].nodes.selectedDataset = getDatasetFromFlow(state.flow[storeKey].nodes.definition, type, key, state.flow[storeKey].nodes.renamedDatasets)

                    if (type === trac.FlowNodeType.INPUT_NODE) {
                        state.flow[storeKey].nodes.selectedNodeSearchQuery = getSearchQueryFromFlowNode(state.flow[storeKey].nodes.definition, key)
                    } else {
                        state.flow[storeKey].nodes.selectedNodeSearchQuery = {}
                    }

                    // Blank the model only properties
                    state.flow[storeKey].nodes.selectedModelInputs = {}
                    state.flow[storeKey].nodes.selectedModelOutputs = {}
                }

            } else {

                // Blank all the properties
                state.flow[storeKey].nodes.selectedDataset = {}
                state.flow[storeKey].nodes.selectedNodeSearchQuery = {}
                state.flow[storeKey].nodes.selectedModelInputs = {}
                state.flow[storeKey].nodes.selectedModelOutputs = {}
            }
        },
        /**
         * A reducer that stores the layout of the SVG, these can be used to draw the PDF version of the SVG
         * (defined in the {@link FlowSvgForPdf} component), it saves the props to here so that these props
         * can be used in the {@link ButtonToDownloadPdf} component to render the SVG in a downloadable PDF.
         */
        savePdfVersionOfSvg: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], pdfVersion: PdfChartProps }>) => {

            const {storeKey, pdfVersion} = action.payload

            state.flow[storeKey].pdf.pdfChartProps = pdfVersion
        },
        /**
         * A reducer that runs when the user uses the toolbar to change the direction that the flow SVG is shown. This
         * causes the SVG to be redrawn.
         */
        setFlowDirection: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], flowDirection: "DOWN" | "RIGHT" }>) => {

            const {storeKey, flowDirection} = action.payload

            state.flow[storeKey].chart.flowDirection = flowDirection
        },
        /**
         * A reducer that runs when the user uses the toolbar to toggle between showing or hiding dataset renames in the
         * flow. This is where an output from a model has a different name to that used when it is passed as an input
         * to another model - in which case the flow has a node to represent this renaming.
         */
        setShowRenames: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], showRenames: boolean }>) => {

            const {storeKey, showRenames} = action.payload

            state.flow[storeKey].chart.showRenames = showRenames
        },
        /**
         * A reducer that runs when the user uses the toolbar to change the tool that they want to use.
         */
        setSelectedTool: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], selectedTool: { real: "none" | "pan" | "zoom-in" | "zoom-out", override: "none" | "pan" | "zoom-in" | "zoom-out" } }>) => {

            const {storeKey, selectedTool} = action.payload

            state.flow[storeKey].chart.selectedTool = selectedTool
        },
        /**
         * A reducer that runs when the settings for the flow SVG are changed, for example when the user zooms in. The
         * settings are stored here so that if the user moves away from the scene and then comes back then the same
         * settings can be used.
         */
        saveSvgSettings: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], svgSettings: Value }>) => {

            const {storeKey, svgSettings} = action.payload

            console.log(storeKey)
            console.log(svgSettings)
            state.flow[storeKey].svg.settings = svgSettings
            state.flow[storeKey].svgParametersLoadedFromComponent = true
        },
        /**
         * A reducer that takes a tag for a flow and extracts information needed to be able to navigate the flow. So
         * this lists the datasets in the flow both without and with taking account of renames. This is run when a
         * new flow is downloaded to be visualised so has some additional steps to remove settings/cleanup if the
         * new tag is different to the one that the settings are stored for.
         */
        setFlowNodeOptions: (state, action: PayloadAction<{ storeKey: keyof ObjectSummaryStoreState["flow"], tag: trac.metadata.ITag }>) => {

            const {storeKey, tag} = action.payload

            if (tag?.header && tag?.definition?.flow) {

                const newObjectKey = createUniqueObjectKey(tag.header, false)

                // If the settings stored are for a different flow than the tag being passed as an argument then reset the
                // store and then add the new information in
                if (newObjectKey !== state.flow[storeKey].objectKey) {
                    state.flow[storeKey] = addFlowUser()

                    const {definition: {flow}} = tag
                    state.flow[storeKey].objectKey = createUniqueObjectKey(tag.header, false)
                    state.flow[storeKey].nodes.optionsWithoutRenames = convertFlowNodesToOptions(flow, false)
                    state.flow[storeKey].nodes.optionsWithRenames = convertFlowNodesToOptions(flow, true)
                    state.flow[storeKey].nodes.renamedDatasets = getRenamedDatasets(flow)
                    state.flow[storeKey].nodes.definition = flow
                }
            }
        }
    }
})

// Action creators are generated for each case reducer function
export const {
    savePdfVersionOfSvg,
    saveSvgSettings,
    setFlowDirection,
    setFlowNodeOptions,
    setSelectedNodeId,
    setSelectedTool,
    setShowRenames,
    setViewTagSelector
} = objectSummaryStore.actions

export default objectSummaryStore.reducer