/**
 * A component that shows a progress bar, this can be used as a placeholder while making async calls.
 * The user is provided with dots that fill as the async calls complete.
 *
 * @module ProgressBar
 * @category Component
 */
import PropTypes from "prop-types";
import React from "react";

// The number of circles to show is always 10
const circles = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

/**
 * An interface for the props of the ProgressBar component.
 */
export interface Props {

    /**
     * The css class to apply to the bar, this allows additional styles to be added to the component.
     * @defaultValue 'my-5'
     */
    className?: string
    /**
     * The number of calls in the process that have competed.
     */
    completed: number
    /**
     * The text to show with the progress icon. If false then no text will show.
     * @defaultValue 'Loading... please wait'
     */
    text?: string | false,
    /**
     * The total number of calls in whatever process is running.
     */
    toDo: number
}

export const ProgressBar = (props: Props) => {

    const {
        className = "my-5",
        completed,
        text = "Loading... please wait",
        toDo
    } = props

    // Don't show anything if text is false
    const finalText = text === false ? null : text

    return (

        <div className={className}>

            <div className={`d-flex justify-content-center`}>
                {circles.map(circle => {

                    const fraction = circles.length * completed / (toDo || 1)
                    const element = Math.floor(fraction)

                    return (
                        <div key={circle}
                             className={`progress-ring ${element === circle ? "running" : element > circle ? "loaded" : "not-loaded"}`}>
                            <span className={`${fraction - Math.floor(fraction) >= 0.5 ? "half" : ""}`}/>
                        </div>
                    )
                })}
            </div>

            {finalText && <div className={"loading-text"}>{finalText}</div>}
        </div>
    )
};

ProgressBar.propTypes = {

    className: PropTypes.string,
    completed: PropTypes.number.isRequired,
    text: PropTypes.oneOfType([PropTypes.string, PropTypes.bool]),
    toDo: PropTypes.number.isRequired
};