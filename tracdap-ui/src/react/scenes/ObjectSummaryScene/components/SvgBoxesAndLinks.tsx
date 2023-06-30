/**
 * A module of components that draw boxes and links SVG flow diagrams in both the browser and the PDF flow documentation
 * @module SvgBoxesAndLinks
 * @category Component
 */

import {ElkEdgeSection, ElkGraphElement, ElkLabel, ElkPoint} from "elkjs/lib/elk-api";
import {G, Path, Rect, Text} from "@react-pdf/renderer";
import {getThemeColour} from "../../../utils/utils_general";
import {LabelSizes} from "../../../../types/types_general";
import React from "react";
import {SVGPresentationAttributes} from "@react-pdf/types";
import {tracdap as trac} from "@finos/tracdap-web-api";

// These are SVG paths for icons in the rectangle boxes, they are extracted from the icon set used (Bootstrap icons)
// The free to use string are available on their website e.g. https://icons.getbootstrap.com/icons/code-slash/
const tableIconPath = "M0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V2zm15 2h-4v3h4V4zm0 4h-4v3h4V8zm0 4h-4v3h3a1 1 0 0 0 1-1v-2zm-5 3v-3H6v3h4zm-5 0v-3H1v2a1 1 0 0 0 1 1h3zm-4-4h4V8H1v3zm0-4h4V4H1v3zm5-3v3h4V4H6zm4 4H6v3h4V8z"
const codeIconPath = "M10.478 1.647a.5.5 0 1 0-.956-.294l-4 13a.5.5 0 0 0 .956.294l4-13zM4.854 4.146a.5.5 0 0 1 0 .708L1.707 8l3.147 3.146a.5.5 0 0 1-.708.708l-3.5-3.5a.5.5 0 0 1 0-.708l3.5-3.5a.5.5 0 0 1 .708 0zm6.292 0a.5.5 0 0 0 0 .708L14.293 8l-3.147 3.146a.5.5 0 0 0 .708.708l3.5-3.5a.5.5 0 0 0 0-.708l-3.5-3.5a.5.5 0 0 0-.708 0z"

// A lookup between the type of box enum and the icon to show, this is embedded in the box id.
const iconLookup = {
    // Not set or intermediate
    0: tableIconPath,
    // Input
    1: tableIconPath,
    // Output
    2: tableIconPath,
    // Model
    3: codeIconPath
}

/**
 * A function that returns an SVG path for an arrow pointing in one of four directions.
 * @param x - The x position of the center of the arrow.
 * @param y - The y position of the center of the arrow.
 * @param width - The width the arrow.
 * @param height - The height the arrow.
 * @param direction - The direction of the arrow.
 * @returns - A SVG path.
 */
const getArrowPath = (x: number, y: number, width: number, height: number, direction: "left" | "right" | "up" | "down"): string => {

    let halfWidth, halfHeight

    if (direction === "down") {

        halfWidth = Math.round(width / 2)
        halfHeight = Math.round(height / 2)

        return `M ${x - halfWidth}, ${y - halfHeight} L ${x + halfWidth}, ${y - halfHeight} L ${x}, ${y + halfHeight} L ${x - halfWidth}, ${y - halfHeight} z`

    } else if (direction === "right") {

        halfWidth = Math.round(height / 2)
        halfHeight = Math.round(width / 2)

        return `M ${x - halfWidth}, ${y - halfHeight} L ${x - halfWidth}, ${y + halfHeight} L ${x + halfWidth}, ${y} L ${x - halfWidth}, ${y - halfHeight} z`

    } else if (direction === "left") {

        halfWidth = Math.round(height / 2)
        halfHeight = Math.round(width / 2)

        return `M ${x + halfWidth}, ${y + halfHeight} L ${x + halfWidth}, ${y - halfHeight} L ${x - halfWidth}, ${y} L ${x + halfWidth}, ${y + halfHeight} z`

    } else if (direction === "up") {

        halfWidth = Math.round(width / 2)
        halfHeight = Math.round(height / 2)
        return `M ${x + halfWidth}, ${y + halfHeight} L ${x - halfWidth}, ${y + halfHeight} L ${x}, ${y - halfHeight} L ${x + halfWidth}, ${y + halfHeight} z`

    } else {

        throw new TypeError(`Arrow direction ${direction} is not recognised`)
    }
}

/**
 * A function that returns the css styles for the links in the SVG
 */
const getSvgStyles = (fontSize: number, type: null | trac.FlowNodeType): Record<string, React.CSSProperties & SVGPresentationAttributes> => {

    // This function gets the hexadecimal colour code not the css variable. This is so the css
    // shows correctly in the downloaded/PDF image
    // TODO remove this when theme is updated
    const primaryText = getThemeColour("lightTheme", "--primary-text")
    const secondaryText = getThemeColour("lightTheme", "--secondary-text")
    const tertiaryText = getThemeColour("lightTheme", "--tertiary-text")
    const danger = getThemeColour("lightTheme", "--danger")
    const info = getThemeColour("lightTheme", "--info")
    const infoDark = getThemeColour("lightTheme", "--info-dark")
    const success = getThemeColour("lightTheme", "--success")
    const successDark = getThemeColour("lightTheme", "--success-dark")
    const primaryBackground = getThemeColour("lightTheme", "--primary-background")
    const secondaryBackground = getThemeColour("lightTheme", "--secondary-background")

    const cssLookup = {

        rect: {
            stroke: {
                // Not set or intermediate
                0: tertiaryText,
                // Input
                1: successDark,
                // Output
                2: infoDark,
                // Model
                3: primaryText
            },
            fill: {
                0: secondaryBackground,
                // Input
                1: success,
                // Output
                2: info,
                // Model
                3: secondaryText
            }
        },
        icon: {
            fill: {
                // Not set or intermediate
                0: primaryText,
                // Input
                1: primaryBackground,
                // Output
                2: primaryBackground,
                // Model
                3: primaryBackground
            }
        }
    }

    return ({
        lineAttrs: {
            stroke: tertiaryText, 'strokeWidth': 1, 'fill': 'none'
        },
        renameLineAttrs: {
            stroke: danger, 'strokeWidth': 1, 'fill': 'none'
        },
        arrowAttrs: {
            stroke: tertiaryText, 'strokeWidth': 1, 'fill': tertiaryText
        },
        renameArrowAttrs: {
            stroke: danger, 'strokeWidth': 1, 'fill': danger
        },
        lineTextAttrs: {
            fill: danger,
            "alignmentBaseline": "hanging",
            "dominantBaseline": "hanging",
            "fontSize": `${fontSize}px`,
            "strokeWidth": "1",
            "fontFamily": "Helvetica"
        },
        rectAttrs: {
            stroke: cssLookup.rect.stroke[type || 0],
            fill: cssLookup.rect.fill[type || 0],
            'strokeWidth': "0.5",
        },
        iconAttrs: {
            fill: cssLookup.icon.fill[type || 0],
            'strokeWidth': 0
        },
        rectTextAttrs: {
            fill: cssLookup.icon.fill[type || 0],
            'alignmentBaseline': "hanging",
            "dominantBaseline": "hanging",
            "fontSize": `${fontSize}px`,
            "strokeWidth": "1",
            "fontFamily": "Helvetica"
        }
    })
}

/**
 * A component that returns a link between two points in the flow chart These links can consist of
 * multiple lines that do not necessarily make a straight line. If there is enough space then we add an arrow to
 * each link to show the direction of the relationship. If the link is flagged as relating to a renamed dataset
 * an additional label is added and the link is coloured red.
 * @param startPoint - The start point of the path. This is provided by the ELK plugin.
 * @param endPoint - The end point of the path. This is provided by the ELK plugin.
 * @param bendPoints- The array of points in the path that break it up into sections. This is provided by
 * the ELK plugin.
 * @param xOffset - The x-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param yOffset - The y-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param arrowLength - The length of the arrow to show. The width is based as a proportion of this.
 */
export const getLinkCoordinates = (startPoint: ElkPoint, endPoint: ElkPoint, bendPoints: undefined | ElkPoint[], xOffset: number, yOffset: number, arrowLength: number): {
    linePath: string
    arrow: {
        showArrow: boolean
        arrowPath: string
        x: number
        y: number
        direction: "left" | "right" | "up" | "down"
        lineLength: number
    }
} => {

    // The start and end points for the lines are in different properties of the section so here
    // we process them into an array with each element containing all the info for the line.
    let lines = []

    if (!bendPoints) {

        // Without any bend points it is just a straight line
        lines.push({
            startX: startPoint.x + xOffset,
            startY: startPoint.y + yOffset,
            endX: endPoint.x + xOffset,
            endY: endPoint.y + yOffset
        })

    } else {

        // With bend points there are any number of elbows
        lines.push({
            startX: startPoint.x + xOffset,
            startY: startPoint.y + yOffset,
            endX: bendPoints[0].x + xOffset,
            endY: bendPoints[0].y + yOffset
        })

        bendPoints.forEach((bendPoint, i) => {
            if (i !== bendPoints.length - 1) {
                lines.push({
                    startX: bendPoint.x + xOffset,
                    startY: bendPoint.y + yOffset,
                    endX: bendPoints[i + 1].x + xOffset,
                    endY: bendPoints[i + 1].y + yOffset
                })
            }
        })

        lines.push({
            startX: bendPoints[bendPoints.length - 1].x + xOffset,
            startY: bendPoints[bendPoints.length - 1].y + yOffset,
            endX: endPoint.x + xOffset,
            endY: endPoint.y + yOffset
        })
    }

    // What are the lengths of the lines
    let lineLengths = lines.map(line => Math.max(Math.abs(line.endX - line.startX), Math.abs(line.endY - line.startY)))

    // Which is the one that's the largest, if its large enough it is the longest one wel add an arrow to
    const maxLength = Math.max(...lineLengths)

    // Turn the array into an array of booleans for whether an arrow head will fit on each line
    // Which link in the edge is the one to add the arrow to
    const indexToAddArrow = lineLengths.map(lineLength => lineLength === maxLength && lineLength > arrowLength * 1.5).findIndex(lineLength => lineLength)

    // This sets defaults too, if there is no arrow built below these aren't used
    let arrowPath: string = "", direction: "left" | "right" | "up" | "down" = "up"
    let arrowX: number = 0, arrowY: number = 0

    // This is -1 if no links are candidates for an arrow
    if (indexToAddArrow > -1) {

        const {endX, endY, startX, startY} = lines[indexToAddArrow]

        // We need to do the vertical or horizontal line test as a relative difference, occasionally the ELK positions for the
        // start and end differ at like the 10th decimal place for small diagrams but this gets bigger the larger the SVG.
        if (startX <= endX && Math.abs(startY / endY) > 0.99 && Math.abs(startY / endY) < 1.01) {
            direction = "right"
            arrowX = startX + (endX - startX) / 2
            arrowY = startY
        } else if (startX > endX && Math.abs(startY / endY) > 0.99 && Math.abs(startY / endY) < 1.01) {
            direction = "left"
            arrowX = startX + (endX - startX) / 2
            arrowY = startY
        } else if (startY > endY && Math.abs(startX / endX) > 0.99 && Math.abs(startX / endX) < 1.01) {
            direction = "up"
            arrowX = startX
            arrowY = startY + (endY - startY) / 2
        } else if (startY <= endY && Math.abs(startX / endX) > 0.99 && Math.abs(startX / endX) < 1.01) {
            direction = "down"
            arrowX = startX
            arrowY = startY + (endY - startY) / 2
        } else {
            arrowX = startX
            arrowY = startY
        }

        arrowPath = getArrowPath(arrowX, arrowY, arrowLength * 0.6, arrowLength, direction)
    }

    const linePath = lines.map((line, i) => (
        i === 0 ? `M ${line.startX}, ${line.startY} L ${line.endX}, ${line.endY}` : ` L ${line.endX}, ${line.endY}`
    )).join("")

    return {
        linePath,
        arrow: {
            showArrow: Boolean(indexToAddArrow > -1 && arrowPath && direction),
            arrowPath,
            x: arrowX,
            y: arrowY,
            direction,
            lineLength: indexToAddArrow > -1 ? lineLengths[indexToAddArrow] : 0
        }
    }
}

/**
 * A component that returns a link between two points in the flow chart in the browser. There is an equivalent
 * component that creates the same item but for the PDF plugin (react-pdf). If you change this function
 * then ensure that the browser version is also updated.
 *
 * These links can consist of multiple lines that do not necessarily make a straight line. If there is enough
 * space then we add an arrow to each link to show the direction of the relationship. If the link is flagged
 * as relating to a renamed dataset an additional label is added and the link is coloured red.
 *
 * @param section - The link details from ELK, including the start and end points and any bends.
 * @param xOffset - The x-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param yOffset - The y-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param arrowLength - The length of the arrow to show. The width is based as a proportion of this.
 * @param renameLine - Whether the link relates to a renamed dataset.
 * @param fontSize - The fontsize of any labels attached to the links.
 * @param labelSizes - The dimensions of the labels inside the boxes keyed by the label. These
 * were extracted from a dummy SVG added to the DOM. It also contains the label dimensions for the renamed
 * link text.
 * @param endClass - An array of node Ids that are connected out of a node, we add classes to the
 * links based off of these so that if the user selects a node we can also select all the links into it
 * and classes to them to make them highlighted.
 * @param startClass - An array of node Ids that are connected out of a node, we add classes to the
 * links based off of these so that if the user selects a node we can also select all the links out of it
 * and classes to them to make them highlighted.
 */

/**
 * An interface for the props of the SvgLinkForBrowser component.
 */
export interface SvgLinkForBrowserProps {
    section: ElkEdgeSection,
    xOffset: number
    yOffset: number
    arrowLength: number
    renameLine: boolean
    fontSize: number
    labelSizes: null | LabelSizes
    endClass: string[],
    startClass: string[]
}

export const SvgLinkForBrowser = ({
                                      section,
                                      xOffset,
                                      yOffset,
                                      arrowLength,
                                      renameLine,
                                      fontSize,
                                      labelSizes,
                                      endClass,
                                      startClass
                                  }: SvgLinkForBrowserProps): React.ReactElement => {

    // Get the details of the link to draw
    const {startPoint, endPoint, bendPoints} = section

    // Get the styles for the links
    const {lineAttrs, renameLineAttrs, arrowAttrs, renameArrowAttrs, lineTextAttrs} = getSvgStyles(fontSize, null)

    // Calculate the coordinated of the link and any arrows and labels
    const {linePath, arrow} = getLinkCoordinates(startPoint, endPoint, bendPoints, xOffset, yOffset, arrowLength)

    return (
        <React.Fragment>
            <path d={linePath} style={renameLine ? renameLineAttrs : lineAttrs}
                  className={`${startClass.map(key => `trac-ui-svg-line-${key}`).join(" ")} ${endClass.map(key => `trac-ui-svg-line-${key}`).join(" ")}`}/>

            {arrow.showArrow &&
                <path d={arrow.arrowPath}
                      style={renameLine ? renameArrowAttrs : arrowAttrs}
                      className={`trac-ui-svg-arrow ${startClass.map(key => `trac-ui-svg-line-${key}`).join(" ")} ${endClass.map(key => `trac-ui-svg-line-${key}`).join(" ")}`}/>
            }

            {arrow.showArrow && ["down", "up"].includes(arrow.direction) && renameLine && labelSizes && arrow.lineLength > labelSizes["Rename"].height &&
                <text x={arrow.x + arrowLength * 0.6}
                      y={arrow.y - fontSize / 2 - 1}
                      style={lineTextAttrs}
                >
                    Rename
                </text>
            }

            {arrow.showArrow && ["left", "right"].includes(arrow.direction) && renameLine && labelSizes && arrow.lineLength > labelSizes["Rename"].width &&
                <text x={arrow.x - labelSizes["Rename"].width / 2}
                      y={arrow.y + 0.6 * arrowLength / 2 + 1}
                      style={lineTextAttrs}
                >
                    Rename
                </text>
            }
        </React.Fragment>
    )
}

/**
 * A component that returns a link between two points in the flow chart in a PDF. There is an equivalent
 * component that creates the same image but for the PDF plugin (react-pdf). If you change this function
 * then ensure that the PDF version is also updated.
 *
 * These links can consist of multiple lines that do not necessarily make a straight line. If there is enough
 * space then we add an arrow to each link to show the direction of the relationship. If the link is flagged
 * as relating to a renamed dataset an additional label is added and the link is coloured red.
 *
 * @param section - The link details from ELK, including the start and end points and any bends.
 * @param xOffset - The x-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param yOffset - The y-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param arrowLength - The length of the arrow to show. The width is based as a proportion of this.
 * @param renameLine - Whether the link relates to a renamed dataset.
 * @param fontSize - The fontsize of any labels attached to the links.
 * @param labelSizes - The dimensions of the labels inside the boxes keyed by the label. These
 * were extracted from a dummy SVG added to the DOM. It also contains the label dimensions for the renamed
 * link text.
 */

/**
 * An interface for the props of the SvgLinkForPdfBrowser component.
 */
export interface SvgLinkForPdfProps {

    arrowLength: number
    fontSize: number
    labelSizes: null | LabelSizes
    renameLine: boolean
    section: ElkEdgeSection,
    xOffset: number
    yOffset: number
}

export const SvgLinkForPdf = ({section, xOffset, yOffset, arrowLength, renameLine, fontSize, labelSizes}: SvgLinkForPdfProps): React.ReactElement => {

    // Get the details of the link to draw
    const {startPoint, endPoint, bendPoints} = section

    // Get the styles for the links
    const {lineAttrs, renameLineAttrs, arrowAttrs, renameArrowAttrs, lineTextAttrs} = getSvgStyles(fontSize, null)

    // Calculate the coordinated of the link and any arrows and labels
    const {linePath, arrow} = getLinkCoordinates(startPoint, endPoint, bendPoints, xOffset, yOffset, arrowLength)

    return (
        <React.Fragment>
            <Path d={linePath} style={renameLine ? renameLineAttrs : lineAttrs}/>

            {arrow.showArrow &&
                <Path d={arrow.arrowPath}
                      style={renameLine ? renameArrowAttrs : arrowAttrs}/>
            }

            {arrow.showArrow && ["down", "up"].includes(arrow.direction) && renameLine && labelSizes && arrow.lineLength > labelSizes["Rename"].height &&
                <Text x={arrow.x + arrowLength * 0.6}
                      y={arrow.y - fontSize / 2 - 1}
                      style={lineTextAttrs}
                >
                    Rename
                </Text>
            }

            {arrow.showArrow && ["left", "right"].includes(arrow.direction) && renameLine && labelSizes && arrow.lineLength > labelSizes["Rename"].width &&
                <Text x={arrow.x - labelSizes["Rename"].width / 2}
                      y={arrow.y + 0.6 * arrowLength / 2 + 1}
                      style={lineTextAttrs}
                >
                    Rename
                </Text>
            }
        </React.Fragment>
    )
}

/**
 * A component that returns a rectangle box in the flow chart in the browser. There is an equivalent
 * component that creates the same item but for the PDF plugin (react-pdf). If you change this function
 * then ensure that the browser version is also updated.
 *
 * @param id - The ID of the box.
 * @param type - The type of box, e.g. input, model or output.
 * @param x - The x position of the top left corner of the rectangle.
 * @param y - The y position of the top left corner of the rectangle.
 * @param width - The width of the rectangle. This takes account of the label size so that it fits.
 * @param height - The height of the rectangle.
 * @param label - The label details including the text to add to the box.
 * @param xOffset - The x-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param yOffset - The y-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param fontSize - The fontsize of any labels attached to the links.
 * @param onSvgItemClick - The function to run when the user clicks on a box.
 * @param toolReal - The tool selected by the user in the toolbar.
 */

/**
 * An interface for the props of the SvgBoxForBrowser component.
 */
export interface SvgBoxForBrowserProps {
    id: string
    type?: trac.FlowNodeType
    x: undefined | number
    y: undefined | number
    width: undefined | number
    height: undefined | number
    label: undefined | (ElkLabel & ElkGraphElement)
    xOffset: number
    yOffset: number
    fontSize: number
    onSvgItemClick?: (e: React.MouseEvent<SVGGElement>) => void
    toolReal: "none" | "pan" | "zoom-in" | "zoom-out"
}

export const SvgBoxForBrowser = ({
                                     id,
                                     type,
                                     x = 0,
                                     y = 0,
                                     width = 0,
                                     height = 0,
                                     label,
                                     xOffset,
                                     yOffset,
                                     fontSize,
                                     onSvgItemClick,
                                     toolReal
                                 }: SvgBoxForBrowserProps) => {

    // 0 type is type not set
    const {rectAttrs, iconAttrs, rectTextAttrs} = getSvgStyles(fontSize, type || trac.FlowNodeType.NODE_TYPE_NOT_SET)

    return (
        // The class here makes sure in zoom in mode the click event pointer is replaced with a pointer icon
        <g className={toolReal === "zoom-in" ? "zoom-in" : toolReal === "none" ? "pointer" : ""}
           onClick={onSvgItemClick}>
            <rect id={id}
                  x={x + xOffset}
                  y={y + yOffset}
                  width={width}
                  height={height}
                  rx={height / 2}
                  ry={height / 2}
                  style={rectAttrs}
            />
            <path id={id}
                  d={iconLookup[type || 0]}
                  style={{
                      ...iconAttrs, ...{
                          // There is a scaling factor that gets the images to fit into the rectangle nicely
                          transform: `translate(${(x + 10 + xOffset)}px, ${y + yOffset + height / 4}px) scale(${1}, ${1})`,
                      }
                  }}
            />
            {label && label.x != null && label.y != null &&
                <text id={id}
                      x={x + label.x - 5 + xOffset}
                    // The +2 is a shift to make the text look more vertically centered
                      y={y + yOffset + label.y + 2}
                      style={rectTextAttrs}
                >
                    {label.text}
                </text>
            }
        </g>
    )
}

/**
 * A component that returns a rectangle box in the flow chart in a PDF. There is an equivalent
 * component that creates the same image but for the PDF plugin (react-pdf). If you change this function
 * then ensure that the PDF version is also updated.
 *
 * @param type - The type of box, e.g. input, model or output.
 * @param x - The x position of the top left corner of the rectangle.
 * @param y - The y position of the top left corner of the rectangle.
 * @param width - The width of the rectangle. Takes account of the label size so that it fits.
 * @param height - The height of the rectangle.
 * @param label - The label details including the text to add to the box.
 * @param xOffset - The x-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param yOffset - The y-offset calculated to make the SVG image have the same size ratio as the
 * viewer container.
 * @param fontSize - The fontsize of any labels attached to the links.
 */

/**
 * An interface for the props of the SvgBoxForBrowser component.
 */
export interface SvgBoxForPdfProps {
    fontSize: number
    height: undefined | number
    label: undefined | (ElkLabel & ElkGraphElement)
    type?: trac.FlowNodeType
    width: undefined | number
    x: undefined | number
    xOffset: number
    y: undefined | number
    yOffset: number
}

export const SvgBoxForPdf = ({
                                 fontSize,
                                 height = 0,
                                 label,
                                 type,
                                 width = 0,
                                 x = 0,
                                 xOffset,
                                 y = 0,
                                 yOffset
                             }: SvgBoxForPdfProps) => {

    const {rectAttrs, iconAttrs, rectTextAttrs} = getSvgStyles(fontSize, type || trac.FlowNodeType.NODE_TYPE_NOT_SET)

    return (
        <G>
            <Rect x={x + xOffset}
                  y={y + yOffset}
                  width={width}
                  height={height}
                  rx={height / 2}
                  ry={height / 2}
                  style={rectAttrs}
            />
            <Path d={iconLookup[type || 0]}
                  style={{
                      ...iconAttrs, ...{
                          // There is a scaling factor that gets the images to fit into the rectangle nicely
                          transform: `translate(${(x + 10 + xOffset)}px, ${y + yOffset + height / 4}px) scale(${1}, ${1})`,
                      }
                  }}
            />
            {label && label.x != null && label.y != null &&
                <Text x={x + label.x - 5 + xOffset}
                    // The +2 is a shift to make the text look more vertically centered due
                      y={y + yOffset + label.y + 2}
                      style={rectTextAttrs}
                >
                    {label.text}
                </Text>
            }
        </G>
    )
}