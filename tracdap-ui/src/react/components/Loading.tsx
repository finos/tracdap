/**
 * A component that shows a loading icon, this is used when waiting for API responses to complete.
 * The animated icon is css based.
 *
 * @module Loading
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the Layout component.
 */
export interface Props {

    /**
     * The css class to apply to the icon, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The text to show with the wait icon. If true the default text will show.
     */
    text?: string | false,
}

export const Loading = (props: Props) => {

    const {
        className = "my-5",
        text = "Loading... please wait"
    } = props;

    // Don't show anything if text is false
    const finalText = text === false ? null : text

    return (
        <div className={className}>
            <div className={"loading-spinner"}>
                <div className="loading-box box1">
                    <div className="circle"/>
                </div>
                <div className="loading-box box2">
                    <div className="circle"/>
                </div>
                <div className="loading-box box3">
                    <div className="circle"/>
                </div>
                <div className="loading-box box4">
                    <div className="circle"/>
                </div>
                <div className="loading-box box5">
                    <div className="circle"/>
                </div>
                <div className="loading-box box6">
                    <div className="circle"/>
                </div>
                <div className="loading-box box7">
                    <div className="circle"/>
                </div>
            </div>
            {finalText && <div className={"loading-text mt-2"}>{finalText}</div>}
        </div>
    )
};

Loading.propTypes = {

    className: PropTypes.string,
    text: PropTypes.oneOfType([PropTypes.string, PropTypes.bool]),
};