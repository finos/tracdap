import {KeyLabels, LabelSizes} from "../../../../types/types_general";
import React, {forwardRef, memo} from "react";
import {SvgBoxForBrowser} from "./SvgBoxesAndLinks";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A component that shows a key for the flow boxes
 */

type Props = {

    /**
     * The horizontal padding in the boxes in the flow chart.
     */
    boxXPadding: number
    /**
     * The vertical padding in the boxes in the flow chart.
     */
    boxYPadding: number
    /**
     * The size of font in pixels for the labels in the flow chart.
     */
    fontSize: number
    /**
     * The text for the labels in the key.
     */
    keyLabels: KeyLabels
    /**
     * The sizes of all the text to show in the boxes.
     */
    labelSizes: null | LabelSizes
};

const FlowKeyForBrowser = forwardRef((props: Props, ref: React.LegacyRef<SVGSVGElement>) => {

    const {
        boxXPadding,
        boxYPadding,
        fontSize,
        keyLabels,
        labelSizes
    } = props

    // THe default box size is quite large so e use this scaling factor to reduce the size of the key in the browser
    const scalingFactor = 1.2

    // The +2 is for a 1 pixel buffer around the SVG as indicated by the x=1 and y=1 props below
    const width = labelSizes ? 2 + (4 * boxXPadding) + (3 * 10) + labelSizes[keyLabels.output].width + labelSizes[keyLabels.model].width + labelSizes[keyLabels.input].width + labelSizes[keyLabels.intermediate].width : 1
    const height = labelSizes ? labelSizes[keyLabels.input].height + (boxYPadding * 2) + 2 : 0

    return (
        <React.Fragment>
            {labelSizes &&

                <svg id="key-svg"
                     width={width}
                     className={"mt-2"}
                     // The +2 is for a 1 pixel buffer around the SVG as indicated by the x=1 and y=1 props below
                     height={height}
                     // The +2 is for a 1 pixel buffer around the SVG as indicated by the x=1 and y=1 props below
                     viewBox={`0 0 ${scalingFactor * width} ${scalingFactor * height}`}
                     ref={ref}
                >
                    <SvgBoxForBrowser id={keyLabels.input}
                                      type={trac.FlowNodeType.INPUT_NODE}
                                      x={1}
                                      y={1}
                                      width={labelSizes[keyLabels.input].width + boxXPadding}
                                      height={labelSizes[keyLabels.input].height + boxYPadding * 2}
                                      label={labelSizes[keyLabels.input] ? {
                                          id: keyLabels.input,
                                          width: labelSizes[keyLabels.input].width,
                                          height: labelSizes[keyLabels.input].height,
                                          text: keyLabels.input,
                                          // We don't have the x and y because we didn't ELK to solve for the graph
                                          // SO we graph the values that it gives
                                          x: 40,
                                          y: boxYPadding
                                      } : undefined}
                                      xOffset={0}
                                      yOffset={0}
                                      fontSize={fontSize}
                                      toolReal={"pan"}
                    />
                    <SvgBoxForBrowser id={keyLabels.output}
                                      type={trac.FlowNodeType.OUTPUT_NODE}
                                      x={1}
                                      y={1}
                                      width={labelSizes[keyLabels.output].width + boxXPadding}
                                      height={labelSizes[keyLabels.output].height + boxYPadding * 2}
                                      label={labelSizes[keyLabels.output] ? {
                                          id: keyLabels.input,
                                          width: labelSizes[keyLabels.output].width,
                                          height: labelSizes[keyLabels.output].height,
                                          text: keyLabels.output,
                                          // We don't have the x and y because we didn't ELK to solve for the graph
                                          // SO we graph the values that it gives
                                          x: 40,
                                          y: boxYPadding
                                      } : undefined}
                                      xOffset={labelSizes[keyLabels.input].width + boxXPadding + 10}
                                      yOffset={0}
                                      fontSize={fontSize}
                                      toolReal={"pan"}
                    />
                    <SvgBoxForBrowser id={keyLabels.model}
                                      type={trac.FlowNodeType.MODEL_NODE}
                                      x={1}
                                      y={1}
                                      width={labelSizes[keyLabels.model].width + boxXPadding}
                                      height={labelSizes[keyLabels.model].height + boxYPadding * 2}
                                      label={labelSizes[keyLabels.model] ? {
                                          id: keyLabels.model,
                                          width: labelSizes[keyLabels.model].width,
                                          height: labelSizes[keyLabels.model].height,
                                          text: keyLabels.model,
                                          // We don't have the x and y because we didn't ELK to solve for the graph
                                          // SO we graph the values that it gives
                                          x: 40,
                                          y: boxYPadding
                                      } : undefined}
                                      xOffset={labelSizes[keyLabels.input].width + labelSizes[keyLabels.output].width + 2 * boxXPadding + 20}
                                      yOffset={0}
                                      fontSize={fontSize}
                                      toolReal={"pan"}
                    />
                    <SvgBoxForBrowser id={keyLabels.intermediate}
                        // Made up type for intermediate, not part of TRAC API
                                      type={trac.FlowNodeType.NODE_TYPE_NOT_SET}
                                      x={1}
                                      y={1}
                                      width={labelSizes[keyLabels.intermediate].width + boxXPadding}
                                      height={labelSizes[keyLabels.intermediate].height + boxYPadding * 2}
                                      label={labelSizes[keyLabels.intermediate] ? {
                                          id: keyLabels.intermediate,
                                          width: labelSizes[keyLabels.intermediate].width,
                                          height: labelSizes[keyLabels.intermediate].height,
                                          text: keyLabels.intermediate,
                                          // We don't have the x and y because we didn't ELK to solve for the graph
                                          // SO we graph the values that it gives
                                          x: 40,
                                          y: boxYPadding
                                      } : undefined}
                                      xOffset={labelSizes[keyLabels.input].width + labelSizes[keyLabels.output].width + labelSizes[keyLabels.model].width + 3 * boxXPadding + 30}
                                      yOffset={0}
                                      fontSize={fontSize}
                                      toolReal={"pan"}
                    />

                </svg>
            }
        </React.Fragment>
    )
})

export default memo(FlowKeyForBrowser)