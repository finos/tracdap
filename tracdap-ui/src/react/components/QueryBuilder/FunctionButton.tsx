/**
 * A button component for the functions that are available to apply to variables in the query.
 *
 * @module FunctionButton
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {Button} from "../Button";
import type {ButtonPayload} from "../../../types/types_general";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the FunctionButton component.
 */
export interface Props {

    /**
     * Whether the button is disabled, it is disabled if the function can not be applied to the chosen variable.
     */
    disabled?: boolean
    /**
     * The icon to show instead of the label.
     */
    icon?: string
    /**
     * The type of button e.g. 'aggregation'
     */
    id: "aggregation" | "sqlFunction" | "unique"
    /**
     * The label to show in the button.
     */
    label?: string
    /**
     * The name of the operator. This should be the SQL name e.g 'between' or '>'.
     */
    name: string
    /**
     * The function to run when the user clicks on the button.
     */
    onClick: ActionCreatorWithPayload<ButtonPayload>
    /**
     *  Whether the button is 'on'.
     */
    selected: boolean
}

export const FunctionButton = ({id, name, onClick, selected, label, icon, disabled}: Props) => (

        <Button active={selected}
                ariaLabel={name}
                className={"mb-3 mb-md-2 fs-13 min-width-px-30"}
                disabled={disabled}
                dispatchedOnClick={onClick}
                id={id}
                name={name}
                size={"sm"}
                variant={"outline-secondary"}
        >
            {icon ? <Icon ariaLabel={false} icon={icon}/> : label ? label : "Unknown"}
        </Button>
);

FunctionButton.propTypes = {

    disabled: PropTypes.bool,
    icon: PropTypes.string,
    id: PropTypes.string.isRequired,
    label: PropTypes.string,
    name: PropTypes.string.isRequired,
    onClick: PropTypes.func.isRequired,
    selected: PropTypes.bool.isRequired
};