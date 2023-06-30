import {GraphLayout, LabelSizes} from "../../../../types/types_general";
import React, {forwardRef, memo} from "react";
import {SvgBoxForBrowser, SvgLinkForBrowser} from "./SvgBoxesAndLinks";

/**
 * A component that calculates the SVG elements of the flow chart in a browser. This maps across all the children (boxes)
 * and edges (links) in the flow and draws the rectangles and lines to the SVG. It is a memoized component in order
 * to prevent re-renders. This component is for viewing in a PDF, there is a sister component called FlowSvgForBrowser
 * (src\react\scenes\ObjectSummaryScene\components\FlowSvgForBrowser.tsx) that is for viewing in a browser. These two
 * components need to be kept in sync so if a change is made to one then it should be reflected in the other.
 */

type Props = {

    /**
     * The major length of arrows on the lines on the flow chart.
     */
    arrowLength: number
    /**
     * The size of font in pixels for the labels in the flow chart.
     */
    fontSize: number
    /**
     * The nodes and edges with the information required to render each box and link.
     */
    graphLayout: GraphLayout
    /**
     * The sizes of all the text to show in the boxes.
     */
    labelSizes: null | LabelSizes
    /**
     * A function that runs when the user clicks on a box when in the select mode in the toolbar.
     */
    onSvgItemClick: (e: React.MouseEvent<SVGGElement>) => void
    /**
     * The tool selected in the toolbar, this is not passed to the SVG pan and zoom plugin but this is the
     * real tool selected by the user.
     */
    toolReal: "none" | "pan" | "zoom-in" | "zoom-out"
};

const FlowSvgForBrowser = forwardRef((props: Props, ref: React.LegacyRef<SVGGElement>) => {

    const {
        graphLayout,
        arrowLength,
        fontSize,
        labelSizes,
        onSvgItemClick,
        toolReal,
    } = props

    const {children, edges} = graphLayout.g

    // Get the x offSet needed to centre the SVG in the viewer
    const {xOffset, yOffset} = graphLayout.dimensions

    return (
        <g ref={ref} id={"svg-g-container"}>
            {/* For each link in the flow */}
            {edges && edges.map((edge) => (

                    edge.sections && edge.sections.map((section, i) => (

                        <SvgLinkForBrowser key={i}
                                           section={section}
                                           xOffset={xOffset}
                                           yOffset={yOffset}
                                           arrowLength={arrowLength}
                            // We stuffed an additional property into the edges that is not part of the ELK interface
                                           renameLine={Boolean(edge.isRename)}
                                           fontSize={fontSize}
                                           labelSizes={labelSizes}
                                           endClass={edge.targets?? []}
                                           startClass={edge.sources?? []}
                        />
                    ))
                )
            )}

            {/* For each node in the flow */}
            {children && children.map(child => (

                <SvgBoxForBrowser key={child.id}
                                  id={child.id}
                    // We stuffed an additional property into the edged that is not part of the ELK interface
                                  type={child.type}
                                  x={child.x}
                                  y={child.y}
                                  width={child.width}
                                  height={child.height}
                                  label={child.labels && child.labels.length > 0 ? child.labels[0] : undefined}
                                  xOffset={xOffset}
                                  yOffset={yOffset}
                                  fontSize={fontSize}
                                  onSvgItemClick={onSvgItemClick}
                                  toolReal={toolReal}
                />
            ))}
        </g>
    )
})

export default memo(FlowSvgForBrowser)