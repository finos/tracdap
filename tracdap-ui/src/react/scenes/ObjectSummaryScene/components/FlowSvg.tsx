import Col from "react-bootstrap/Col";
import {downloadSvg} from "../../../utils/utils_downloads";
import {GraphLayout, GregsEdge, KeyLabels, LabelSizes, RenamedDataset} from "../../../../types/types_general";
import {ElkCommonDescription, ElkExtendedEdge, ElkNode} from 'elkjs/lib/elk-api';
import ELK from "elkjs/lib/elk.bundled";
import FlowKeyForBrowser from "./FlowKeyForBrowser";
import FlowSvgForBrowser from "./FlowSvgForBrowser";
import {getAdditionalEdgesForRenamedDatasets, getRenamedDatasets} from "../../../utils/utils_flows";
import {isDefined} from "../../../utils/utils_trac_type_chckers";
import NoFlow from "./NoFlow";
import {ObjectSummaryStoreState, savePdfVersionOfSvg, setSelectedNodeId, saveSvgSettings} from "../store/objectSummaryStore";
import PropTypes from "prop-types";
import React, {useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState} from "react";
import {ReactSVGPanZoom, ToolbarPosition, Value, ViewerMouseEvent} from 'react-svg-pan-zoom';
import Row from "react-bootstrap/Row";
import SvgToolbar from "./SvgToolbar";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {whichMouseButtonClicked} from "../../../utils/utils_general";

// Note that the types for the react-svg-pan-zoom plugin is not for the latest version, instead it is an update for
// version 2. This means that some type information is not right e.g. the Value type. This use of the types
// papers over these differences but in reality the types need updating.

// Labels for the key in below the SVG
const keyLabels: KeyLabels = {
    input: "Input data",
    output: "Output data",
    model: "Model",
    intermediate: "Intermediate data"
}

// This is just a dummy function to pass as props to the ReactSVGPanZoom where they are required,
// but we don't want to use them
const dummyFunction = () => {
}

// Initialise a new instance of ELK, this will work out the paths and the positions
// of all the items in the flow to show in the process. Note that if the ELK package is slow then you can
// add a web worker to improve performance. You can pass an argument to define using a worker.
// {
//     workerUrl: './node_modules/elkjs/lib/elk-worker.min.js'
// }
const elk = new ELK()

// Props to the ReactSVGPanZoom component that we use to turn off some default
// components (such as the toolbar) so we can replace them with this app's bespoke versions
const toolbarProps: { position: ToolbarPosition } = {position: "none"}

// We need to pass the position 'none' to hide the miniature
const miniatureProps: { position: "none" | "right" | "left", background: string, width: number, height: number } = {
    position: "none",
    background: "#616264",
    width: 100,
    height: 80
}

// The height of the browser view port.
const vh = Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0)

// This is for a blank column so that the layout with a vertical toolbar is symmetric. The min
// width needs to be set to the width of the toolbar
const blankColStyle: React.CSSProperties = {minWidth: "50px"}

// This is a small buffer that may be needed to specify the width of the SVG viewer so that we can put columns
// either side that do not force the viewer to be moved to a new line.
const bufferPx = 0

/**
 * A function to process a node definition, a node is a box in the flow chart.
 * This returns a box definition that can be used by ELK and have it solve for the
 * positions of all the boxes and the links.
 *
 * @param edge - An edge from a flow definition metadata.
 * @param which - Whether we are getting the start or the end part of an edge.
 * @param boxYPadding - The y-axis padding to apply to the box.
 * @param boxXPadding - The x-axis padding to apply to the box.
 * @param labelSizes - The dimensions of the labels inside the boxes keyed by the label. These
 * were extracted from a dummy SVG added to the DOM.
 */
const getGraphNode = (edge: GregsEdge, which: "start" | "end", boxYPadding: number, boxXPadding: number, labelSizes: LabelSizes): { type: trac.FlowNodeType } & ElkNode & ElkCommonDescription => {

    return {
        id: `${edge[which].type}-${edge[which].node}`,
        name: edge[which].node,
        height: labelSizes[edge[which].node].height + boxYPadding * 2,
        width: labelSizes[edge[which].node].width + boxXPadding,
        type: edge[which].type,
        labels: [
            {
                id: edge[which].node,
                text: edge[which].node,
                width: labelSizes[edge[which].node].width,
                height: labelSizes[edge[which].node].height
            }
        ],
        layoutOptions: {
            'nodeLabels.placement': 'INSIDE V_CENTER H_RIGHT'
        }
    }
}

/**
 * A component that shows a flow as a process chart. The chart can be zoomed into, nodes selected
 * and their metadata viewed.
 */

type Props = {

    /**
     * The major length of arrows on the lines on the flow chart.
     */
    arrowLength: number
    /**
     * The css class to apply to the SVG, this allows additional styles to be added to the component.
     */
    className: string
    /**
     * Whether the SVG can be exported.
     */
    canExport: boolean
    /**
     * The TRAC flow tag to show the process map of.
     */
    flow: trac.metadata.ITag
    /**
     * The size of font in pixels for the labels in the flow chart.
     */
    fontSize: number
    /**
     * The margins to leave around the SVG when drawn.
     */
    margin: { left: number, right: number, top: number, bottom: number }

    /**
     * The horizontal padding in the boxes in the flow chart.
     */
    boxXPadding: number
    /**
     * The vertical padding in the boxes in the flow chart.
     */
    boxYPadding: number
    /**
     * The key in the objectSummaryStore to get the state for this component
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
};

const FlowSvg = (props: Props) => {

    console.log("Rendering FlowSvg")

    const {
        arrowLength,
        boxXPadding,
        boxYPadding,
        canExport,
        className,
        flow,
        fontSize,
        margin,
        storeKey
    } = props

    // Get what we need from the store
    const {
        flowDirection,
        selectedNodeId,
        selectedTool,
        showRenames
    } = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].chart)

    const {settings: svgSettingsProps} = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].svg)

    // Has a save of this SVG's settings been made to the store
    const {svgParametersLoadedFromComponent} = useAppSelector(state => state["objectSummaryStore"].flow[storeKey])

    // Get what we need from the store, this is used in the name of the
    // file when we are exporting the SVG
    const {login: {userId}} = useAppSelector(state => state["applicationStore"])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // The container div that the SVG is drawn in - to get the width
    const container = useRef<HTMLDivElement>(null);

    // The SVGPanAndZoom plugin
    const viewer = useRef<null | (ReactSVGPanZoom & { ViewerDOM: Node })>(null);

    // The sizes of the text strings for the labels in the flow
    // We add these into a dummy SVG to get their sizes and then
    // delete the SVG - so we need to keep the values, but they
    // should not cause a rerender
    const labelSizes = useRef<null | LabelSizes>(null)

    // The SVG in the DOM
    const svgRef = useRef<null | SVGGElement>(null)

    // The dimensions of the SVG, we need this to know when to refit the SVG to the window, in this case its
    // acting like prevProps
    const modifiedDimensionsRef = useRef<null | { modifiedHeight: number, modifiedWidth: number }>(null)

    // This is an optimisation on the useEffect that checks against selectedNodeId
    // actually having changed
    const prevPropSelectedNodeId = useRef<null | string>(selectedNodeId)

    // This is the computed positions and paths of the items in the SVG calculated by ELK
    const [graphLayout, setGraphLayout] = useState<null | GraphLayout>(null)

    /**
     * A function that sets the viewer to fit the whole of the SVG into it. It acts like a reset function.
     * We need this because we are using the controlled version of the SVG plugin - so need to control it.
     */
    const fitToViewer = useCallback(() => {

        if (viewer.current) return viewer.current.fitToViewer();

    }, [])

    // The state of the table excluding the state used internally by the react-table plugin
    const [svgSettingsState, setSvgSettingsState] = useState<null | Value>(svgParametersLoadedFromComponent ? svgSettingsProps : null)

    /**
     * A function that runs when the user clicks on the SVG toolbar menu to download the SVG. The critical
     * point here is that we are passing the reference to the SVG in the DOM to the download function which
     * passes the image to use for the download.
     */
    const handleSvgDownload = useCallback(() => {

        if (svgRef.current && flow && svgSettingsState !== null) downloadSvg(svgRef.current, flow, userId, svgSettingsState)

    }, [flow, svgSettingsState, userId])

    /**
     * Awesomeness. A hook that runs when the component unmounts and that sends the state of the SVG
     * to a Redux store. This allows the component to re-mounted with the same state and the same view.
     */
    // This is componentWillUnmount, see https://stackoverflow.com/questions/55020041/react-hooks-useeffect-cleanup-for-only-componentwillunmount
    const componentWillUnmount = useRef(false)

    useEffect(() => () => {
        componentWillUnmount.current = true
    }, [])

    useEffect(() => {

        return () => {

            // Strictly only ever run this when the component unmounts
            if (componentWillUnmount.current && svgSettingsState !== null) {
                console.log("LOG :: Saving SVG state")
                dispatch(saveSvgSettings({storeKey, svgSettings: svgSettingsState}))
            }
        }

    }, [dispatch, storeKey, svgSettingsState])

    // Is the state holding settings loaded from the redux store (set on the first render only)
    const oldSettingsStoredToState = useRef<boolean>(svgSettingsState !== null)

    /**
     * A function that runs when the settings for the SVG change. This function is passed as a prop to the
     * ReactSVGPanZoom component which it runs when the component's internal settings change (pan, zoom etc.),
     * this allows us to store these outside this component - this means that we can make it so that when
     * a user revisits this component the SVG can be loaded in exactly the same state as it was left in.
     */
    const handleSvgSettingsChange = useCallback((settings: Value) => {

        // If the state of the SVG is from a newly mounted SVG (svgSettingsState === null) or
        // one that has just been newly mounted and the user has changed a setting

        // Or this is not the first render of a previously mounted. We need this as the handleSvgSettingsChange
        // function runs when the ReactSVGPanZoom mounts, saving the visualisation parameters to the state.
        // However, this would overwrite the values that were pulled from the Redux store, so we need a way
        // of identifying this initial update and not storing it to state.

        if (!oldSettingsStoredToState.current) {

            setSvgSettingsState(settings)
        }

            // This is the magic, only disallow the first attempt to overwrite the state settings when they are
        // holding a previous version from the store. Then they will be in sync.
        else if (oldSettingsStoredToState.current) {

            oldSettingsStoredToState.current = false
        }

    }, [])


    /**
     * A function that gets an array of the names or labels of the boxes in the flow chart, this is the keys
     * of all the models and inputs in the flow. We do this so that we can create a dummy SVG with these labels
     * in the same font as the final flow SVG. Then we can get the sizes of these labels and use that in the
     * flow SVG to ensure that the boxes are all the right sizes for their labels.
     */
    const names = useMemo((): string[] => {

        // Other than the names of the boxes in the flow we also have other labels that this component will add,
        // for example if output datasets are mapped as inputs in a second model but are renamed. So here we add these
        // labels in to the list to render into the dummy SVG, so we also have their dimensions. We also add in the
        // labels for the key.
        let names: string[] = ["Rename", ...Object.values(keyLabels)]

        // Go through each edge and map it to an array of four elements, then add these the names array
        flow?.definition?.flow?.edges?.map(edge => [edge.source?.node, edge.target?.node, edge.source?.socket, edge.target?.socket]).forEach(temp => {
            names = [...names, ...temp.filter(isDefined)]
        })

        // Make a unique list
        return [...new Set(names)]

    }, [flow?.definition?.flow])


    // An array of information about datasets in the flow that are renamed, the output of a model has a
    // different key when it gets used as the input to another model (the flow will contain info that
    // links the two names)
    const renamedDatasets: RenamedDataset[] = useMemo(() => getRenamedDatasets(flow?.definition?.flow), [flow])

    /**
     * A function that takes the flow metadata and converts the edges (links in the flow) and
     * children (nodes in the flow) into the structure needed by the ELK plugin that will use
     * these items to solve for the positions and paths of all the items in the flow so that
     * they can be drawn in an SVG.
     */
    const buildGraphStructure = useCallback((labelSizes: LabelSizes): { edges: ElkExtendedEdge[], children: ElkNode[] } => {

        if (!flow?.definition?.flow?.nodes || !flow?.definition?.flow?.edges) return {edges: [], children: []}

        const {edges, nodes} = flow?.definition?.flow

        let gregsEdges: GregsEdge[] = []

        // First calculate the links in the graph where we ignore any datasets that are renamed so that the renaming
        // isn't visible to the user. Edges are links between nodes in the flow definition
        edges?.forEach(edge => {

            // The node is the name of the dataset or model node
            const sourceNode = edge.source?.node
            const targetNode = edge.target?.node

            // The type is the corresponding type in node definition
            const sourceNodeType = sourceNode ? nodes[sourceNode].nodeType : undefined
            const targetNodeType = targetNode ? nodes[targetNode].nodeType : undefined

            // MODEL to MODEL links where a dataset is being passed directly as an output to an input
            // In the SVG we do not want to link a model to another model directly so for these edges we replace the model
            // at one end with the dataset going in as an input. Model to model links require the socket and node
            // to be set
            if (sourceNode && targetNode && edge.source?.socket && edge.target?.socket && sourceNodeType === trac.FlowNodeType.MODEL_NODE && targetNodeType === trac.FlowNodeType.MODEL_NODE) {

                gregsEdges.push({
                    id: `e${gregsEdges.length}`,
                    // This will be an output node, if the dataset is not set as an output node then it will be
                    // undefined - which equates as an intermediate dataset
                    sources: [`${nodes[edge.source.socket] ? nodes[edge.source.socket].nodeType : trac.FlowNodeType.NODE_TYPE_NOT_SET}-${edge.source.socket}`],
                    targets: [`${trac.FlowNodeType.MODEL_NODE}-${targetNode}`],
                    start: {
                        node: edge.source.socket,
                        type: nodes[edge.source.socket] ? nodes[edge.source.socket].nodeType || trac.FlowNodeType.NODE_TYPE_NOT_SET : trac.FlowNodeType.NODE_TYPE_NOT_SET
                    },
                    end: {
                        node: targetNode,
                        type: trac.FlowNodeType.MODEL_NODE
                    }
                })

                // If the dataset is not renamed between the two sockets then we need to create a new link from the
                // previous model to the output, the above will be from the output (from the previous model) to the
                // model. However, if there is also an edge just outputting the dataset without passing it to another
                // model then we don't need to add it as it will be the same as the link above.
                if (edge.source.socket === edge.target.socket && !gregsEdges.find(existingEdge => existingEdge.sources[0] === `${trac.FlowNodeType.MODEL_NODE}-${sourceNode}` && existingEdge.targets[0] === `${trac.FlowNodeType.OUTPUT_NODE}-${edge.source?.socket}`)) {

                    gregsEdges.push({
                        id: `e${gregsEdges.length}`,
                        sources: [`${trac.FlowNodeType.MODEL_NODE}-${sourceNode}`],
                        targets: [`${nodes[edge.source.socket] ? nodes[edge.source.socket].nodeType || trac.FlowNodeType.NODE_TYPE_NOT_SET : trac.FlowNodeType.NODE_TYPE_NOT_SET}-${edge.source.socket}`],
                        start: {node: sourceNode, type: trac.FlowNodeType.MODEL_NODE},
                        end: {
                            node: edge.target.socket,
                            type: nodes[edge.source.socket] ? nodes[edge.source.socket].nodeType || trac.FlowNodeType.NODE_TYPE_NOT_SET : trac.FlowNodeType.NODE_TYPE_NOT_SET
                        }
                    })
                }

            } else if (sourceNode && targetNode && sourceNodeType && targetNodeType) {

                // INPUT to MODEL or MODEL to OUTPUT nodes
                gregsEdges.push({
                    id: `e${gregsEdges.length}`,
                    sources: [`${sourceNodeType}-${sourceNode}`],
                    targets: [`${targetNodeType}-${targetNode}`],
                    start: {node: sourceNode, type: sourceNodeType},
                    end: {node: targetNode, type: targetNodeType}
                })
            }
        })

        // Get the additional edges are needed to show the renamed datasets in the graph
        const additionalEdgesForRenames = showRenames ? getAdditionalEdgesForRenamedDatasets(renamedDatasets) : []

        // Splice in the extra links
        gregsEdges = [...gregsEdges, ...additionalEdgesForRenames]

        // We need to remove the links that were added before the splicing of the rename links that correspond tp
        // the same linking between
        showRenames && renamedDatasets.forEach(rename => {

            const removeLinkIndex = gregsEdges.findIndex(edge => edge.sources[0] === `${trac.FlowNodeType.OUTPUT_NODE}-${rename.datasetStart}` && edge.targets[0] === `${trac.FlowNodeType.MODEL_NODE}-${rename.modelEnd}`)

            if (removeLinkIndex > -1) {
                gregsEdges.splice(removeLinkIndex, 1)
            }

        })

        // As well as the edges remapped to the structure required by the ELK plugin it also needs the nodes or
        // children information, so we process those now
        let children: { [key: string]: ElkNode } = {}

        gregsEdges.forEach((edge) => {

            // Add the details to the children object only once
            if (!children.hasOwnProperty(`${edge.start.type}-${edge.start.node}`)) {

                children[`${edge.start.type}-${edge.start.node}`] = getGraphNode(edge, "start", boxYPadding, boxXPadding, labelSizes)
            }

            if (!children.hasOwnProperty(`${edge.end.type}-${edge.end.node}`)) {

                children[`${edge.end.type}-${edge.end.node}`] = getGraphNode(edge, "end", boxYPadding, boxXPadding, labelSizes)
            }
        })

        return {
            edges: gregsEdges,
            children: Object.values(children)
        }

    }, [boxXPadding, boxYPadding, flow?.definition?.flow, renamedDatasets, showRenames])

    /**
     * A hook that runs when the graph layout is recalculated. The graph layout contains all the
     * boxes to show and the links to join them, including their positions and sizes. If this
     * changes then we have to call the SVG plugin method to ensure that the viewer
     * shows the whole SVG. We don't run this if there are settings for the SVG as this means that
     * we are loading from the store, and we already have a zoom to apply.
     */
    useLayoutEffect(() => {

        // Then when the SVG is created the graph layout has a particular width and size for the image. We then need to
        // fit the viewport to show the whole of that image. When the user changes the way that the graph is drawn then
        // the dimensions will change, so we need to reset the SVG to again fill the viewport.
        if (graphLayout && viewer.current && (modifiedDimensionsRef.current?.modifiedHeight !== graphLayout.dimensions.modifiedHeight || modifiedDimensionsRef.current?.modifiedWidth !== graphLayout.dimensions.modifiedWidth)) {

            // Update the ref, so we can check next time if we need to fit the SVG to the viewport
            modifiedDimensionsRef.current = {
                modifiedHeight: graphLayout.dimensions.modifiedHeight,
                modifiedWidth: graphLayout.dimensions.modifiedWidth
            }

            // Reset the SVG visible to the whole image
            viewer.current.fitSelection(0, 0, graphLayout.dimensions.modifiedWidth, graphLayout.dimensions.modifiedHeight)
        }

    }, [fitToViewer, flowDirection, graphLayout, svgSettingsState]);


    const calculateGraphLayout = useCallback(async (flowDirection: "DOWN" | "RIGHT", margin: Props["margin"], names: string[]): Promise<GraphLayout> => {

        const containerWidth = container.current ? container.current["clientWidth"] : 100

        // Get the sizes of the labels in the dummy SVG, we can only
        // do this when the component mounts as after that we've deleted  the SVG
        if (!labelSizes.current) {

            let tempLabelSizes: LabelSizes = {}
            names.forEach(name => {

                const text = document.getElementById(`dummy-svg-${name}`) as SVGTextElement | null
                if (text) tempLabelSizes[name] = {width: text.getBBox().width, height: text.getBBox().height}
            })

            labelSizes.current = tempLabelSizes
        }

        // Build the structure needed to put into ELK
        const {edges, children} = buildGraphStructure(labelSizes.current)

        // The ELK object
        const graph: ElkNode = {
            id: "root",
            layoutOptions: {
                "elk.algorithm": "layered",
                "elk.direction": flowDirection,
                "elk.padding": `[left=${margin.left}, top=${margin.top}, right=${margin.right}, bottom=${margin.bottom}]`,
                "spacing.nodeNode": "50",
                "spacing.nodeNodeBetweenLayers": "50",
            },
            children: children,
            edges: edges
        }

        // Get the positions of the boxes and the paths to link them
        // Store them in state to cause a rerender
        return elk.layout(graph).then(g => {

            if (!g.height || !g.width) throw new TypeError("The SVG has no dimensions")

            let dimensions: GraphLayout["dimensions"] = {
                heightRatio: 0,
                widthRatio: 0,
                containerWidth: 0,
                containerHeight: 0,
                xOffset: 0,
                yOffset: 0,
                modifiedHeight: 0,
                modifiedWidth: 0,
                // Used when rendering the PDF
                originalWidth: 0,
                originalHeight: 0
            }

            // What is the aspect ratio of the SVG image, if this does not fit the aspect ratio of the container
            // then we will add offsets to the SVG
            const svgRatio = g.height / g.width

            // If the SVG has been drawn then we know the height of the container, otherwise we have to calculate what it will be
            // based on the size of the SVG from ELK and the width of the container. Note that the height has min and max sizes set
            // based on the viewport
            const containerHeight = viewer.current ? viewer.current["props"].height : Math.max(450, Math.min(vh * 0.75, (g.height / (g.width)) * ((containerWidth - bufferPx))))
            const containerRatio = containerHeight / (containerWidth - bufferPx)

            let xOffset = 0, yOffset = 0

            dimensions.originalWidth = g.width
            dimensions.originalHeight = g.height
            dimensions.heightRatio = g.height / containerHeight
            dimensions.widthRatio = g.width / containerWidth
            dimensions.containerWidth = containerWidth - bufferPx;
            dimensions.containerHeight = containerHeight;

            if (svgRatio > containerRatio) {
                // The SVG is vertically elongated compared to the container so add x-axis offsets
                xOffset = Math.max(0, (dimensions.heightRatio * containerWidth - bufferPx - g.width) / 2);
            } else if (svgRatio < containerRatio) {
                // The SVG is horizontally elongated compared to the container so add y-axis offsets
                yOffset = Math.max(0, (dimensions.widthRatio * containerHeight - g.height) / 2)
            }

            dimensions.xOffset = xOffset;
            dimensions.yOffset = yOffset
            dimensions.modifiedHeight = g.height + 2 * yOffset
            dimensions.modifiedWidth = g.width + 2 * xOffset

            return {g, dimensions}

        }).catch(err => {

            if (typeof err === "string") {
                throw new Error(err)
            } else {
                console.error(err)
                throw new Error("Creating the SVG graph failed")
            }
        })

    }, [buildGraphStructure])

    /**
     * A hook that runs after the page has loaded, here we query the labels added to a temporary
     * SVG and extract their bounding boxes, so we have the sizes of the labels - they must be
     * drawn with the same font size and family. Then we use these label sizes to get the flow
     * chart positions using ELK. Note that this needs to run after the component loads for
     * the first time as otherwise the dummy SVG will not have rendered.
     */
    useEffect(() => {

        let shouldUpdateLayout = true

        // We can't call async functions with await directly in useEffect, so we have to define our
        // async function wrapper inside.
        // See https://stackoverflow.com/questions/53332321/react-hook-warnings-for-async-function-in-useeffect-useeffect-function-must-ret
        async function myWrapper(flowDirection: "DOWN" | "RIGHT"): Promise<GraphLayout> {
            return await calculateGraphLayout(flowDirection, margin, names)
        }

        myWrapper(flowDirection).then((graphLayout) => {

            if (shouldUpdateLayout) {

                // Update state
                setGraphLayout(graphLayout)

                // We also store a separate version of the SVG layout for the version of the SVG (defined
                // in the FlowSvgForPdf component) this is always shown as flowing downwards, if the user
                // is viewing it downwards
                if (flowDirection === "DOWN" && graphLayout) {

                    const newProps = {
                        graphLayout: {...graphLayout},
                        arrowLength,
                        fontSize,
                        labelSizes: {...labelSizes.current},
                        // Needed for the SVG key in PDF
                        boxXPadding,
                        boxYPadding,
                        keyLabels
                    }

                    dispatch(savePdfVersionOfSvg({storeKey, pdfVersion: newProps}))
                }
            }

        }).then(() => {

            if (shouldUpdateLayout) {
                if (flowDirection !== "DOWN") {

                    /**
                     * Draw the PDF version of the SVG (defined
                     * in the FlowSvgForPdf component) this is always shown as flowing downwards, it saves the props to a store so that these props
                     * can be used in the ButtonToDownloadPdf component to render the SVG in a downloadable PDF.
                     */
                    myWrapper("DOWN").then((graphLayout) => {

                        if (graphLayout) {

                            const newProps = {
                                graphLayout: {...graphLayout},
                                arrowLength,
                                fontSize,
                                labelSizes: {...labelSizes.current},
                                // Needed for the SVG key in PDF
                                boxXPadding,
                                boxYPadding,
                                keyLabels
                            }

                            dispatch(savePdfVersionOfSvg({storeKey, pdfVersion: newProps}))
                        }
                    })
                }
            }
        })

        return () => {
            shouldUpdateLayout = false
        }

    }, [arrowLength, boxXPadding, boxYPadding, calculateGraphLayout, dispatch, flowDirection, fontSize, margin, names, storeKey])

    /**
     * A function that removes any user drawn marquee from the SVG. This is run when the user clicks and holds their
     * left mouse button and dragged across the SVG, then releases their button. It also runs when the user clicks their
     * right mouse button during the drag.
     */
    const removeMarquee = useCallback(() => {

        const marquee = document.getElementById("marquee")
        if (marquee) viewer?.current?.ViewerDOM.removeChild(marquee);

    }, [])

    /**
     * A function that runs when the user clicks the left mouse button when over the SVG. If the user has clicked
     * on the zoom in tool then the function adds a marquee or box on the SVG which allows the user to highlight
     * an area to zoom in to. This function is passed as a prop to the ReactSVGPanZoom plugin component. The
     * scale factor and translation variables are needed since the ReactSVGPanZoom plugin allows for zooming and
     * panning, so we need to take account of the view that we have of the image.
     */
    const handleMouseDown = useCallback(<T, >(e: ViewerMouseEvent<T>) => {

        const {x, y, scaleFactor, originalEvent, translationX, translationY} = e

        if (selectedTool.real === "zoom-in" && viewer.current && whichMouseButtonClicked<T>(originalEvent) === "left") {

            viewer.current.ViewerDOM.addEventListener('mouseleave', removeMarquee)

            let marquee = document.createElementNS("http://www.w3.org/2000/svg", 'rect');

            // The colors here match the marquee in the HighCharts plugin
            marquee.style.stroke = `rgba(69, 114, 167, 1)`;
            marquee.style.fill = `rgba(69, 114, 167, 0.25)`;
            marquee.style.strokeWidth = "0.5";
            marquee.setAttribute("x", `${x * scaleFactor + translationX}`)
            marquee.setAttribute("y", `${y * scaleFactor + translationY}`)
            marquee.setAttribute("width", `0`)
            marquee.setAttribute("height", `0`)
            marquee.setAttribute("id", "marquee")
            marquee.setAttribute("transform", `translate(0, 0)`)

            viewer.current.ViewerDOM.appendChild(marquee)
        }

    }, [removeMarquee, selectedTool.real])

    /**
     * A function that runs when the drags the mouse while the left mouse button is clicked. This function
     * gets the marquee created by the handleMouseDown function and updates its size based on where the mouse
     * pointer is dragged to. This function is passed as a prop to the ReactSVGPanZoom plugin component. The
     * scale factor and translation variables are needed since the ReactSVGPanZoom plugin allows for zooming and
     * panning, so we need to take account of the view that we have of the image.
     */
    const handleMouseDrag = useCallback(<T, >(e: ViewerMouseEvent<T>) => {

        const {x, y, scaleFactor, originalEvent, translationX, translationY} = e

        let marquee = document.getElementById("marquee")

        // Check if a button is being held down
        // See https://stackoverflow.com/questions/2405771/is-right-click-a-javascript-event
        // See https://stackoverflow.com/questions/322378/javascript-check-if-mouse-button-down
        if (marquee && whichMouseButtonClicked(originalEvent) === "left") {

            const startX = parseFloat(marquee.getAttribute("x") || "0")
            const startY = parseFloat(marquee.getAttribute("y") || "0")

            const height = Math.max(1, Math.abs(y * scaleFactor - startY + translationY))
            const width = Math.max(1, Math.abs(x * scaleFactor - startX + translationX))

            marquee.setAttribute("height", `${height}`)
            marquee.setAttribute("width", `${width}`)

            const translateY = Math.min(y * scaleFactor - startY + translationY, 0)
            const translateX = Math.min(x * scaleFactor - startX + translationX, 0)

            marquee.setAttribute("transform", `translate(${translateX}, ${translateY})`)

        } else {

            viewer.current?.ViewerDOM?.removeEventListener('mouseleave', removeMarquee)
            removeMarquee()
        }

    }, [removeMarquee])

    /**
     * A function that runs when the user lifts the left mouse button when over the SVG. If the user has
     * created a marquee then the function gets the marquee coordinates and then uses a function from the
     * ReactSVGPanZoom plugin to zoom into the requested area. This function is passed as a prop to the
     * ReactSVGPanZoom plugin component. The scale factor and translation variables are needed since the
     * ReactSVGPanZoom plugin allows for zooming and panning, so we need to take account of the view that we
     * have of the image. We also have to take account of that fact that the user's marquee is unlikely to
     * have the same aspect ratio as the viewer in the browser, so we add some buffers to the marquee
     * coordinates to make the two have the same height/width ratio.
     */
    const handleMouseUp = useCallback(<T, >(e: ViewerMouseEvent<T> & { value: Value }) => {

        const {scaleFactor, translationX, translationY, value} = e

        let marquee = document.getElementById("marquee")

        if (marquee) {

            const x1 = parseFloat(marquee.getAttribute("x") || "0")
            const y1 = parseFloat(marquee.getAttribute("y") || "0")

            let transform = marquee.getAttribute("transform") || ""
            transform = transform.replace(/[^-\d.,]/g, '');

            let transformAsArray = transform.split(",").map(myString => parseFloat(myString))

            let width = parseFloat(marquee.getAttribute("width") || "0")
            let height = parseFloat(marquee.getAttribute("height") || "0")

            if (viewer.current) viewer.current.ViewerDOM.removeChild(marquee);

            let deltaX = 0, deltaY = 0

            if ((value.viewerWidth / value.viewerHeight) > (width / height)) {

                // If the viewer is horizontally more elongated than the area selected then
                // we need to add more to the area selected to match
                deltaX = (height * value.viewerWidth / value.viewerHeight) - width
                width = width + deltaX

            } else {

                // If the viewer is vertically more elongated than the area selected then
                // we need to add more to the area selected to match
                deltaY = (width * value.viewerHeight / value.viewerWidth) - height
                height = height + deltaY
            }

            if (viewer.current) viewer.current.fitSelection((x1 + transformAsArray[0] - deltaX / 2) / scaleFactor - translationX / scaleFactor, (y1 + transformAsArray[1] - deltaY / 2) / scaleFactor - translationY / scaleFactor, width / scaleFactor, height / scaleFactor)
            removeMarquee()
        }

        viewer?.current?.ViewerDOM?.removeEventListener('mouseleave', removeMarquee)

    }, [removeMarquee])

    /**
     * A function that runs in two instances, the first is when the user clicks on a box in the SVG,
     * in which case the event is passed as the argument, and we can extract the ID of the box clicked
     * on, or the user uses the SelectOption component to select a box to see more information about,
     * in which case the standard payload is passed - which includes the ID of the box. If the box is not
     * fully rendered in the window then the viewer is updated to show it, otherwise the viewer does
     * not move.
     */
    const highlightSvgBox = useCallback((id: null | string, graphLayout: null | GraphLayout, svgSettings: null | Pick<Value, "a" | "e" | "f" | "viewerWidth" | "viewerHeight">) => {

        // See if there is a node already selected
        const outerContainer = document.getElementById("svg-g-container")

        // outerContainer won't exist on the first render before the SVG loads
        if (!outerContainer) return

        // Get all the nodes that currently have classes that make them red
        const oldSelectedNode = outerContainer.querySelector(".red-svg-box") as null | SVGGElement
        const oldConnectedLines = outerContainer.querySelectorAll(".red-svg-line") as NodeListOf<SVGGElement>

        // If there is a selected node set then we need to turn off the class added to
        // show it with a red outline
        if (oldSelectedNode) oldSelectedNode.className.baseVal = ""

        // If there were selected lines with the node then also turn off the class added to make them red
        if (oldConnectedLines) {
            for (let i = 0; i < oldConnectedLines.length; i++) {
                const connectedLine = oldConnectedLines?.item(i)
                if (connectedLine) connectedLine.className.baseVal = connectedLine.className.baseVal.replace(/\bred-svg-line\b/, "")
            }
        }

        // If the user has selected a box that is not the same as the one selected
        // then we need to add a red border and update the state (if it was the same
        // as the one selected then that means that they are deselecting the box)
        // The square bracket gets past a lint bug See: https://stackoverflow.com/questions/37923424/value-assigned-to-primitive-will-be-lost
        if (id) {
            const newSelectedNode = document.getElementById(id) as null | SVGGElement
            if (newSelectedNode) newSelectedNode.className.baseVal = "red-svg-box"

            // Make any links that connect the box to red too
            const connectedLines = document.getElementsByClassName(`trac-ui-svg-line-${id}`) as HTMLCollectionOf<SVGGElement>
            if (connectedLines) {
                for (let i = 0; i < connectedLines.length; i++) {
                    const connectedLine = connectedLines?.item(i)
                    if (connectedLine) connectedLine.className.baseVal = `${connectedLine.className.baseVal} red-svg-line`
                }
            }
        }

        if (!id) return

        // Find the definition of the box clicked on from the graph layout, this contains its
        // position and size. Layout will not be set when this is called to add in the red boxes
        // if the settings for the flow were saved to the Redux store, but the zoom should also
        // have been set right.
        if (graphLayout && svgSettings) {
            const child = graphLayout.g.children?.find(child => child.id === id)

            // If the child we need to center on was not found we can't re-center on it
            if (!child || !child.x || !child.y || !child.height || !child.width) return

            // Now we have to work out whether to move the viewer to show the item selected
            // we do this only if the box clicked on is partially outside the viewer. If a
            // box is entirely in the viewer we don't want to jump around unnecessarily

            // The viewer corner coordinates value.a is the zoom level, value.e and value.f are x and y translates
            const viewerLocation = {
                x1: -svgSettings.e / svgSettings.a,
                y1: -svgSettings.f / svgSettings.a,
                x2: (svgSettings.viewerWidth - svgSettings.e) / svgSettings.a,
                y2: (svgSettings.viewerHeight - svgSettings.f) / svgSettings.a
            }

            // The selected box coordinates
            const boxLocation = {
                x1: child.x + graphLayout.dimensions.xOffset,
                y1: child.y + graphLayout.dimensions.yOffset,
                x2: child.x + graphLayout.dimensions.xOffset + child.width,
                y2: child.y + graphLayout.dimensions.yOffset + child.height
            }

            // Do we have to move - if yes recenter on the box - yes all of this had to be worked out
            if (viewer.current && (viewerLocation.x2 < boxLocation.x2 || viewerLocation.x1 > boxLocation.x1 || viewerLocation.y2 < boxLocation.y2 || viewerLocation.y1 > boxLocation.y1)) {
                viewer.current.setPointOnViewerCenter(child.x + child.width / 2 + graphLayout.dimensions.xOffset, child.y + child.height / 2 + graphLayout.dimensions.yOffset, svgSettings.a)
            }
        }

    }, [])

    // This records whether the state settings actually came from saved versions from the store and a node had been selected
    // We need to track this because if this happens then the selected node won't be shown (the red highlighting to show what
    // is selected is triggered by an onClick). So here we track whether we need to simulate the click event. However, a
    // further issue is that on the first render the SVG is not in the DOM, so we can only ever simulate the click after the
    // SVG is in the DOM. This is handled beneath the useRef in the useEffect.
    const hasSelectedNodeOnLoadNotShownInSvg = useRef<boolean>(Boolean(oldSettingsStoredToState.current && selectedNodeId))
    // We need a separate variable rather than using the ref because we need to add it to the dependency array
    const isSvgInDom = Boolean(svgRef.current !== null)

    useEffect(() => {

        if (isSvgInDom && hasSelectedNodeOnLoadNotShownInSvg.current) {
            highlightSvgBox(selectedNodeId, null, null)
            // Make it so this function only ever runs once after the component mounts
            hasSelectedNodeOnLoadNotShownInSvg.current = false
        }

    }, [highlightSvgBox, selectedNodeId, isSvgInDom])

    /**
     * A function that runs when the user clicks on a box in the SVG, in which case we can extract the ID of the box
     * clicked on. We pass the id to the function that updates the selected node.
     */
    const onSvgItemClick = useCallback((e: React.MouseEvent<SVGGElement>) => {

        if (selectedTool.real !== "none") return

        // See: https://www.designcise.com/web/tutorial/how-to-fix-property-does-not-exist-on-type-eventtarget-typescript-error
        const target = e.target as SVGGElement;

        // See if there is a node already selected
        const outerContainer = document.getElementById("svg-g-container")
        const oldSelectedNode = outerContainer && outerContainer.querySelector(".red-svg-box")

        // Get the ID of the item clicked on
        const id = !oldSelectedNode || (oldSelectedNode && oldSelectedNode["id"] !== target.id) ? target.id : null

        // Now update the store.
        // We don't have to highlight the box as it will be done when the setSelectedNodeId
        // function will pass a new selectedNodeId value and there is a hook that will
        // mean that the highlightSvgBox function will run
        dispatch(setSelectedNodeId({storeKey, selectedNodeId: id}))

    }, [storeKey, dispatch, selectedTool.real])

    /**
     * A hook that keeps props and states synced together. If the prop that defines a selected node in
     * the SVG is updated then this hook updates the highlighted box. It uses the prevPropSelectedNodeId
     * ref as an optimisation as the function runs whenever any one of the deps is changed, but we only
     * want to actually run the function when propSelectedNodeId changes.
     */
    useEffect(() => {

        if (svgSettingsState && prevPropSelectedNodeId.current !== selectedNodeId && graphLayout) {

            highlightSvgBox(selectedNodeId, graphLayout, {
                a: svgSettingsState.a,
                e: svgSettingsState.e,
                f: svgSettingsState.f,
                viewerWidth: svgSettingsState.viewerWidth,
                viewerHeight: svgSettingsState.viewerHeight
            })

            // Update the ref to the new value - this is an optimisation
            prevPropSelectedNodeId.current = selectedNodeId
        }

    }, [graphLayout, highlightSvgBox, selectedNodeId, selectedTool, svgSettingsState])

    return (
        <div className={className}>
            <Row className={"d-block d-md-none"}>
                <Col className={"mb-4"}>
                    <SvgToolbar fitToViewer={fitToViewer}
                                toolbarDirection={"horizontal"}
                                canExport={canExport}
                                downloadSvg={handleSvgDownload}
                                hasRenames={Boolean(renamedDatasets.length > 0)}
                                storeKey={storeKey}
                    />
                </Col>
            </Row>

            <Row>
                <Col className={"d-flex justify-content-between p-md-0"}>
                <div style={blankColStyle} className={"d-none d-md-block flex-shrink-0"}>&nbsp;</div>
                <div className={"w-100"}>
                    <div ref={container} className={"w-100 border"}>
                        {flow && graphLayout &&
                            <ReactSVGPanZoom ref={viewer}
                                             width={graphLayout.dimensions.containerWidth}
                                             height={graphLayout.dimensions.containerHeight}
                                             tool={selectedTool.override}
                                // There is a bug in the plugin, null is not allowed for the value,
                                // but an empty object is OK See: https://github.com/chrvadala/react-svg-pan-zoom/blob/main/docs/migrate-from-v2-to-v3.md
                                // If state is null we gat around this by passing a dummy set of settings from the store, the state will be overwritten
                                // will be updated by the handleSvgSettingsChange as a hook after the ReactSVGPanZoom mounts

                                             value={svgSettingsState === null ? svgSettingsProps : svgSettingsState}
                                             onChangeValue={handleSvgSettingsChange}
                                             onChangeTool={dummyFunction}
                                             background={"#fff"}
                                             scaleFactorMax={10}
                                             toolbarProps={toolbarProps}
                                             detectAutoPan={false}
                                             miniatureProps={miniatureProps}
                                             onMouseDown={handleMouseDown}
                                             onMouseMove={handleMouseDrag}
                                             onMouseUp={handleMouseUp}
                                             className={selectedTool.real === "zoom-in" ? "zoom-in" : ""}
                                             detectWheel={false}
                            >
                                <svg width={graphLayout.dimensions.modifiedWidth}
                                     height={graphLayout.dimensions.modifiedHeight}
                                     xmlns={"http://www.w3.org/2000/svg"}
                                >
                                    <FlowSvgForBrowser graphLayout={graphLayout}
                                                       arrowLength={arrowLength}
                                                       fontSize={fontSize}
                                                       ref={svgRef}
                                                       labelSizes={labelSizes.current}
                                                       onSvgItemClick={onSvgItemClick}
                                                       toolReal={selectedTool.real}
                                    />
                                </svg>
                            </ReactSVGPanZoom>
                        }
                    </div>

                    <div className={"d-inline-block w-100"}>
                        <span className={"me-3"}>Key:</span>
                        <FlowKeyForBrowser boxXPadding={boxXPadding}
                                           boxYPadding={boxYPadding}
                                           fontSize={fontSize}
                                           keyLabels={keyLabels}
                                           labelSizes={labelSizes.current}
                        />
                    </div>


                </div>
                <div style={blankColStyle} className={"d-none d-md-block flex-shrink-0"}>
                    <SvgToolbar fitToViewer={fitToViewer}
                                toolbarDirection={"vertical"}
                                canExport={canExport}
                                downloadSvg={handleSvgDownload}
                                hasRenames={Boolean(renamedDatasets.length > 0)}
                                storeKey={storeKey}
                    />
                </div>
                </Col>
            </Row>

            {/*This adds an SVG into the DOM with the labels for the SVG we want to draw, we do this to get their sizes */}
            {/*into labelSizes which means we know how big the boxes should be*/}
            {flow && !graphLayout &&
                <svg id="dummy-svg" width={"100%"} className={"d-hidden"} height={names.length * 50}>
                    {names.map(
                        (name, i) => <text key={name} id={`dummy-svg-${name}`} textAnchor={"left"} x={0}
                                           y={(i + 1) * 25} style={{
                            "fontSize": `${fontSize}px`,
                            "strokeWidth": "1",
                            "fontFamily": "Helvetica"
                        }}>{name}</text>
                    )}
                </svg>
            }

            {!flow &&
                <NoFlow/>
            }

        </div>
    )
};

FlowSvg.propTypes = {

    arrowLength: PropTypes.number,
    boxYPadding: PropTypes.number,
    boxXPadding: PropTypes.number,
    canExport: PropTypes.bool,
    className: PropTypes.string,
    flow: PropTypes.object.isRequired,
    fontSize: PropTypes.number,
    margin: PropTypes.shape({
        top: PropTypes.number,
        left: PropTypes.number,
        bottom: PropTypes.number,
        right: PropTypes.number
    }),
    storeKey: PropTypes.string.isRequired
};

FlowSvg.defaultProps = {

    arrowLength: 10,
    boxYPadding: 10,
    boxXPadding: 45,
    canExport: true,
    className: "my-4",
    fontSize: 12,
    margin: {
        top: 20,
        bottom: 20,
        left: 10,
        right: 10
    }
};

export default FlowSvg;