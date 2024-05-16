/**
 * A component that calculates the SVG elements of the flow chart in a PDF. This maps across all the children (boxes)
 * and edges (links) in the flow and draws the rectangles and lines to the SVG. This component is for viewing in a PDF,
 * there is a sister component called {@link FlowSvgForBrowser} that is for viewing in a browser. These two components
 * need to be kept in sync so if a change is made to one then it should be reflected in the other.
 *
 * @module FLowSvgForPdf
 * @category Component
 */

import {G, StyleSheet, Svg} from "@react-pdf/renderer";
import type {GraphLayout, LabelSizes} from "../../../types/types_general";
import {PdfCss} from "../../../config/config_pdf_css";
import React from "react";
import {SvgBoxForPdf, SvgLinkForPdf} from "../../scenes/ObjectSummaryScene/components/SvgBoxesAndLinks";

// Create the css styles for the exported PDF elements
const styles = StyleSheet.create(PdfCss)

// Some code to make sure that Typescript is happy that we have numbers for the css properties
const pagePaddingLeft = typeof styles.page.paddingLeft === "number" ? styles.page.paddingLeft : 0
const pagePaddingRight = typeof styles.page.paddingRight === "number" ? styles.page.paddingRight : 0
const pagePaddingTop = typeof styles.page.paddingTop === "number" ? styles.page.paddingTop : 0
const pagePaddingBottom = typeof styles.page.paddingBottom === "number" ? styles.page.paddingBottom : 0
const sectionFontSize = typeof styles.section.fontSize === "number" ? styles.section.fontSize : 12
const sectionMarginTop = typeof styles.section.marginTop === "number" ? styles.section.marginTop : 0
const sectionMarginBottom = typeof styles.section.marginBottom === "number" ? styles.section.marginBottom : 0

/**
 * An interface for the props of the FlowSvgForPdf component.
 */
export interface Props {

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
}

export const FlowSvgForPdf = (props: Props) => {

    const {arrowLength, fontSize, graphLayout, labelSizes} = props

    // The default sizes for A4 are 595.28, 841.89 but to get the right size available we have to take account
    // of the margins and padding
    const pageWidth = 595 - pagePaddingLeft - pagePaddingRight
    // The 0.92 is a safety margin to calculating the page height available to make sure the image fits, we also have
    // the key below the main SVG so have to account for that
    const pageHeight = 0.92 * (841 - pagePaddingTop - pagePaddingBottom - sectionFontSize - sectionMarginTop - sectionMarginBottom)

    // Set the dimensions on the page to be full width unless the height would be greater than 1 page
    const svgWidth = (pageWidth * graphLayout.dimensions.originalHeight / graphLayout.dimensions.originalWidth) < pageHeight ? pageWidth : undefined
    const svgHeight = (pageWidth * graphLayout.dimensions.originalHeight / graphLayout.dimensions.originalWidth) < pageHeight ? undefined : pageHeight

    return (
        <Svg width={svgWidth} height={svgHeight}
             viewBox={`0, 0, ${graphLayout.dimensions.originalWidth}, ${graphLayout.dimensions.originalHeight}`}>
            <G>
                {/* For each link in the flow */}
                {graphLayout.g.edges?.map(edge => (
                        edge.sections?.map((section, i) => (

                            <SvgLinkForPdf key={i}
                                           section={section}
                                           xOffset={0}
                                           yOffset={0}
                                           arrowLength={arrowLength}
                                           renameLine={edge.isRename || false}
                                           fontSize={fontSize}
                                           labelSizes={labelSizes}
                            />
                        ))
                    )
                )}

                {/* For each node in the flow */}
                {graphLayout.g.children?.map(child => (

                    <SvgBoxForPdf key={child.id}
                                  type={child.type}
                                  x={child.x}
                                  y={child.y}
                                  width={child.width}
                                  height={child.height}
                                  label={child.labels && child.labels.length > 0 ? child.labels[0] : undefined}
                        // We show the native size in the PDF without the offsets that make the image match the viewer aspect ratio
                                  xOffset={0}
                                  yOffset={0}
                                  fontSize={fontSize}
                    />
                ))}
            </G>
        </Svg>
    )
};