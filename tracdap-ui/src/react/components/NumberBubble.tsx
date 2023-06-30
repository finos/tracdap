/**
 * A component that shows a circle with a number or piece of text in the middle. This is used for lists for example
 * where a more visual element is needed. This can be used for numbers below 100, after that the text will extend
 * beyond the circle.
 *
 * @module NumberBubble
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the NumberBubble component.
 */
export interface Props {

    /**
     * The colour for the background or fill colour of the circle.
     */
    backgroundColour?: string
    /**
     * The colour for the circle.
     */
    circleColour?: string
    /**
     * The colour for the text in the center of the circle.
     */
    fontColour?: string
    /**
     * The width of the line that draws the circle.
     */
    lineWidth?: number
    /**
     * The height of the circle including the circle line width.
     */
    size?: number
    /**
     * The text to show in the centre of the circle.
     */
    text: string | number
}

export const NumberBubble = (props: Props) => {

    const {
        backgroundColour = "var(--primary-background)",
        circleColour = "var(--quaternary-text)",
        fontColour = "var(--tertiary-text)",
        lineWidth = 2,
        size = 28,
        text
    } = props;

    return (

        <svg width={`${size}`} height={`${size}`}>
            {size && lineWidth &&
                <circle cx={`${size / 2}`}
                        cy={`${size / 2}`} r={`${size / 2 - 2 * lineWidth}`}
                        stroke={circleColour}
                        strokeWidth={lineWidth}
                        fill={backgroundColour}
                />
            }
            {/*The extra 4% pushes down the text - which is needed to make it look more central as otherwise it accounts space for g,y and j that is not needed for numbers.*/}
            <text x="50%" y="54%" dominantBaseline="middle" textAnchor="middle" fill={fontColour}>{text}</text>
        </svg>
    )
};

NumberBubble.propTypes = {

    backgroundColour: PropTypes.string,
    circleColour: PropTypes.string,
    fontColour: PropTypes.string,
    lineWidth: PropTypes.number,
    size: PropTypes.number,
    text: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired
};