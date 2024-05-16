/**
 * A component that shows an error message. It can handle lists of errors
 * or one off messages.
 *
 * @module MessageList
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";
import type {Variants} from "../../types/types_general";

/**
 * An interface for the props of the MessageList component.
 */
export interface Props {

    /**
     * Whether the text should be centered, only applies when there is single message.
     */
    centerText: boolean
    /**
     * The css class to apply to the messages, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether the messages if in an array has a header message in the first element. A header does not have a bullet.
     */
    listHasHeader: boolean
    /**
     * The messages to show to the user. If they are in an array they will be displayed as a bullet list unless
     * listHasHeader is true in which case the first message won't have a bullet.
     */
    messages: string | string[]
    /**
     * Whether to show the children as bullet points if they are an array.
     */
    showBullets?: boolean
    /**
     * The type of messages, these should be one of the Bootstrap colour names.
     */
    variant: Variants
}

export const MessageList = (props: Props) => {

    const {centerText, className = "", listHasHeader, messages, showBullets = true, variant} = props

    // The alert-* class applies the alert classes to the component, these are slightly lighter background and darker
    // text than the standard Bootstrap theme colours. The bg-transparent class removes the background colour from the
    // alert-* class so that we just see the variant text (but darker because it's the alert classes)
    const finalClassNameMain = `${className} alert-${variant} bg-transparent`

    let headerText: undefined | string | string[], messageArray: string[]

    // If the user passes a message that is a string rather than an array we treat it internally as a header
    if (typeof messages === "string" || !showBullets) {

        headerText = messages
        messageArray = []

    } else if (listHasHeader && Array.isArray(messages) && messages.length > 0) {

        headerText = messages[0]
        messageArray = messages.slice(1)

    } else {

        messageArray = messages
    }

    const finalClassNameHeader = `${centerText ? "text-center" : ""} ${messageArray.length > 0 ? "mb-2" : ""}`

    return (
        <React.Fragment>
            {(headerText || messageArray) &&
                <div className={finalClassNameMain}>

                    {/*Header message in the array*/}
                    {headerText &&
                        <div className={finalClassNameHeader}>
                            {headerText}
                        </div>
                    }

                    {messageArray && messageArray.length > 0 &&
                        <ul className={"pl-3 mb-0"}>
                            {messageArray.map((message, i) => (
                                <li key={i}>
                                    {message}
                                </li>
                            ))}
                        </ul>
                    }
                </div>
            }
        </React.Fragment>
    )
};

MessageList.propTypes = {

    centerText: PropTypes.bool.isRequired,
    className: PropTypes.string,
    listHasHeader: PropTypes.bool.isRequired,
    messages: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string), PropTypes.string]).isRequired,
    showBullets: PropTypes.bool,
    variant: PropTypes.oneOf(["success", "danger", "warning", "info", "muted", "dark", "light", "white"]).isRequired
};