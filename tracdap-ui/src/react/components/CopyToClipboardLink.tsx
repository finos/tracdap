/**
 * A component that shows a link that when clicked on copies text to the clipboard. There is visual feedback that the
 * text has been successfully copied. Not all organisation's browsers have the function this uses enabled so this
 * component has to check that it is available.
 *
 * @module CopyToClipboardLink
 * @category Component
 */

import {Button} from "./Button";
import Fade from "react-bootstrap/Fade";
import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React, {useEffect, useState} from "react";
import {wait} from "../utils/utils_async";

/**
 * Interface for the props of the CopyToClipboardLink component.
 */
export interface Props {

    /**
     * The text to show in the button.
     */
    buttonText?: string
    /**
     * The text to copy to the clipboard.
     */
    copyText: string | number | boolean
}

/**
 * Interface for the state of the CopyToClipboardLink component.
 */
export interface State {

    /**
     * The colour of the icon set via a class.
     */
    class: "text-danger" | "text-success"
    /**
     * The icon to show.
     */
    icon: "bi-check-circle" | "bi-x-circle",
    /**
     * Whether the icon to show the result of the click is visible.
     */
    iconVisible: boolean,
}

export const CopyToClipboardLink = (props: Props) => {

    const {buttonText = "Copy", copyText} = props;

    const [feedback, setFeedback] = useState<State>({iconVisible: false, icon: "bi-check-circle", class: "text-success"});

    // See https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
    const handleClick = () => navigator?.clipboard?.writeText(copyText.toString()).then(
        // First argument is success and the second is error
        () => setFeedback({iconVisible: true, icon: "bi-check-circle", class: "text-success"}),
        () => setFeedback({iconVisible: true, icon: "bi-x-circle", class: "text-danger"})
    ).catch(error => {

        setFeedback({iconVisible: true, icon: "bi-x-circle", class: "text-danger"})
        console.error(error)
    });

    /**
     * A hook that runs when the visibility of the icon changes, if the icon is visible then it schedules a function to
     * fade it out.
     */
    useEffect(() => {

        if (feedback.iconVisible) wait(750).then(() => setFeedback({
            iconVisible: false,
            icon: "bi-check-circle",
            class: "text-success"
        }))

    }, [feedback.iconVisible])

    return (

        <React.Fragment>

            {/*It looks like some clients turn this function off to harden their browsers, so we need to*/}
            {/*check if the function exists, otherwise we will be showing a link that does nothing*/}
            {navigator?.clipboard?.writeText && typeof navigator.clipboard.writeText === "function" &&
                <React.Fragment>
                    <Fade in={feedback.iconVisible}>
                        {/*The extra span is needed in order to pass the ref required to make the fade work*/}
                        <span className={"me-2"}>
                    <Icon ariaLabel={"Copy to clipboard"}
                          className={feedback.class}
                          colour={null}
                          icon={feedback.icon}
                    />
                </span>
                    </Fade>
                    <Button ariaLabel={"Copy"}
                            className={"py-0"}
                            isDispatched={false}
                            onClick={handleClick}
                            size={"sm"}
                            variant={"link"}
                    >
                        {buttonText}
                    </Button>
                </React.Fragment>
            }

        </React.Fragment>
    )
};

CopyToClipboardLink.propTypes = {

    buttonText: PropTypes.string,
    copyText: PropTypes.string.isRequired
};