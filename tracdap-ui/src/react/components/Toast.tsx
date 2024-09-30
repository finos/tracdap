/**
 * A component that shows a pop-up message or toast to the user. This is used to update the user on events.
 * This is a component passed to the React Toastify plugin allowing us to customise the structure of the message
 * component. There is no formatting for the toast type in this component, that is all handled in the showToast
 * function that uses this component.
 *
 * @module Toast
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";
import {ShowHideDetails} from "./ShowHideDetails";

export interface Props {

    /**
     * The text to show in the toast as details e.g. a technical error message, shown in a hidden area that is expandable.
     */
    details?: string | string[]
    /**
     * The icon to show in the left of the title.
     */
    icon: React.ReactElement<any, any>
    /**
     * The text to show in the toast as the message.
     */
    message: string | React.ReactElement<any, any>
    /**
     * The close icon to show in the right of the title.
     */
    onClose: React.ReactElement<any, any>
    /**
     * The text to show in the toast as a title.
     */
    title?: string
}

export const Toast = (props: Props) => {

    const {details, icon, message, onClose, title} = props

    // Extra information that you don't want to show in the main message, but you want the user to be able to see, for
    // example API error messages
    const subMessages = typeof details === "string" ? <span
        className={"d-block fs-13"}>{details}</span> : Array.isArray(details) ? [...new Set(details)].map((text, i) =>
        <span key={i} className={`d-block fs-13 ${i + 1 < details.length && "mb-3"}`}>{text}</span>) : null

    return (

        <React.Fragment>
            <div className={`d-flex toast-title align-items-center`}>
                <div className={"flex-fill"}>{icon}{title}</div>
                <div>{onClose}</div>
            </div>

            {message && <div className={`toast-message pt-3 ${!subMessages ? "pb-4" : ""}`}>{message}</div>}

            {subMessages &&
                <ShowHideDetails classNameOuter={"pt-2 pb-3"} classNameInner={"pt-2 pb-2"}>
                    {subMessages}
                </ShowHideDetails>
            }
        </React.Fragment>
    )
};

Toast.propTypes = {

    details: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
    icon: PropTypes.element.isRequired,
    message: PropTypes.oneOfType([PropTypes.string, PropTypes.element]).isRequired,
    onClose: PropTypes.element.isRequired,
    title: PropTypes.string
};