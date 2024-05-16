/**
 * A component that shows an alert or a coloured message, this is just a wrapper to the Bootstrap Alert component
 * with the preset classes applied and an adjusted layout to add an icon.
 *
 * @module Alert
 * @category Component
 */

import BootstrapAlert from "react-bootstrap/Alert"
import {Icon} from "./Icon";
import {isStringArray} from "../utils/utils_trac_type_chckers";
import {MessageList} from "./MessageList";
import PropTypes from "prop-types";
import React, {memo} from "react";
import type {Variants} from "../../types/types_general"

// A simple lookup for the icon to show for a given type of alert
const iconLookup: Record<Extract<Variants, "success" | "warning" | "danger" | "info">, string> = {
    success: "bi-check-circle",
    warning: "bi-exclamation-diamond",
    danger: "bi-x-circle",
    info: "bi-info-circle"
}

/**
 * An interface for the props of the Alert component.
 */
export interface Props {

    /**
     * The css class to apply to the alert, this allows additional styles to be added to the component.
     * @defaultValue ''
     */
    className?: string
    /**
     * Whether the messages, if passed as an array, have a header message in the first element. A header does is
     * not shown with a bullet.
     * @defaultValue true
     */
    listHasHeader?: boolean
    /**
     * Whether to show the children as bullet points if they are an array.
     * @defaultValue false
     */
    showBullets?: boolean
    /**
     * The type of alert to show.
     * @defaultValue 'info'
     */
    variant?: keyof typeof iconLookup
}

const AlertInner = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        listHasHeader = false,
        showBullets = true,
        variant = "info"
    } = props;

    return (

        <BootstrapAlert variant={variant} className={`d-inline-block w-100 ${className}`}>
            <div className={"d-flex"}>
                <Icon ariaLabel={"Alert icon"}
                      className={"align-top me-2 pe-1"}
                      icon={iconLookup[variant]}
                />

                {/*If the children are an array of strings then we show this as list of messages. If the children is a
                string or React elements then we render them directly*/}
                {isStringArray(children) ?
                    <MessageList centerText={false}
                                 listHasHeader={listHasHeader}
                                 messages={children}
                                 showBullets={showBullets}
                                 variant={variant}
                    />
                    : children}
            </div>
        </BootstrapAlert>
    )
};

AlertInner.propTypes = {

    children: PropTypes.oneOfType([
        PropTypes.element,
        PropTypes.string,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.element,
                PropTypes.string
            ])
        )]).isRequired,
    className: PropTypes.string,
    listHasHeader: PropTypes.bool,
    showBullets: PropTypes.bool,
    variant: PropTypes.oneOf(["success", "warning", "danger", "info"]),
};

export const Alert = memo(AlertInner);
