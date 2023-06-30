/**
 * A component that shows link that when clicked on shows or hides its children which can be
 * another component that you want to toggle.
 *
 * @module ShowHideDetails
 * @category Component
 */

import {Button} from "./Button"
import Collapse from "react-bootstrap/Collapse";
import {Icon} from "./Icon"
import PropTypes from "prop-types";
import React, {memo, useState} from "react";

export interface Props {

    /**
     * The css class to apply to the child component, this allows additional styles to be added to the inner part of
     * the component.
     */
    classNameInner?: string
    /**
     * The css class to apply to the outer div of this component, combined with classNameInner they allow the
     * overall layout to be set.
     */
    classNameOuter?: string
    /**
     * What icon set to show, arrows or pluses and minuses.
     */
    iconType?: "arrow" | "maths"
    /**
     * The text to show in the show/hide title.
     */
    linkText?: string
    /**
     * What size the link text should be. These are Bootstrap font-size classes.
     */
    linkTextSize?: "fs-1" | "fs-2" | "fs-3" | "fs-4" | "fs-5" | "fs-6" | "fs-7" | "fs-8" | "fs-9" | "fs-10" | "fs-11" | "fs-12"
    /**
     * Whether the children are shown by default
     */
    showOnOpen?: boolean
}

const ShowHideDetailsInner = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        classNameInner = "py-4",
        classNameOuter = "py-4",
        iconType = "arrow",
        linkText = "details",
        linkTextSize,
        showOnOpen = false
    } = props

    // Whether to show the children or not
    const [show, setShow] = useState<boolean>(showOnOpen)

    const finalText = `${show ? "Hide" : "Show"} ${linkText}`

    // This picks the icon based on the set and whether the children components are shown or not
    const finalIcon = show ? (iconType === "arrow" ? "bi-arrow-down-square" : "bi-dash-square") : (iconType === "arrow" ? "bi-arrow-right-square" : "bi-plus-square")

    /**
     * A wrapper function that changes whether the details are shown or not.
     */
    const toggle = (): void => {

        setShow(show => !show)
    }

    return (

        // The details class is used when this component is included in a toast message
        <div className={`details ${classNameOuter}`}>
            <Button ariaLabel={"Show/hide information"}
                    className={`no-halo ps-0 ${linkTextSize || ""}`}
                    onClick={toggle}
                    variant={"link"}
                    isDispatched={false}
            >
                <Icon icon={finalIcon} className={"me-2"} ariaLabel={false}/>{finalText}
            </Button>

            <Collapse in={show}>
                {/*The extra div is needed in order to pass the ref required to make the fade work and to make the animation smooth See https://react-bootstrap.github.io/utilities/transitions/*/}
                <div>
                    <div className={classNameInner}>{children}</div>
                </div>
            </Collapse>
        </div>
    )
};

ShowHideDetailsInner.propTypes = {

    children: PropTypes.oneOfType([
        PropTypes.string,
        PropTypes.element,
        PropTypes.number,
        PropTypes.bool,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.element,
                PropTypes.string,
                PropTypes.bool,
                PropTypes.number,
            ])
        )]),
    classNameInner: PropTypes.string,
    classNameOuter: PropTypes.string,
    iconType: PropTypes.oneOf(["arrow", "maths"]),
    showOnOpen: PropTypes.bool,
    linkText: PropTypes.string,
    linkTextSize: PropTypes.oneOf(["fs-1", "fs-2", "fs-3", "fs-4", "fs-5", "fs-6", "fs-7", "fs-8", "fs-9", "fs-10", "fs-11", "fs-12"])
};

export const ShowHideDetails = memo(ShowHideDetailsInner);