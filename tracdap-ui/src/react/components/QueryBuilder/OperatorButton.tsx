/**
 * A button component for the operators that are available to group variables in the query.
 *
 * @module OperatorButton
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {Button} from "../Button";
import type {ButtonPayload} from "../../../types/types_general";
import {Icon} from "../Icon";
import React from "react";
import PropTypes from "prop-types";

/**
 * An interface for the props of the OperatorButton component.
 */
export interface Props {

    /**
     * The icon to show instead of the label.
     */
    icon?: string
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

export const OperatorButton = ({name, onClick, selected, label, icon}: Props) => (

    <Button active={selected}
            ariaLabel={name}
            className={"me-2 mb-3 mb-md-2 fs-13 min-width-px-30"}
            dispatchedOnClick={onClick}
            id={"operator"}
            name={name}
            size={"sm"}
            variant={"outline-secondary"}
    >
        {icon ? <Icon ariaLabel={false} icon={icon}/> : label ? label : "Unknown"}
    </Button>

);

OperatorButton.propTypes = {

    icon: PropTypes.string,
    label: PropTypes.string,
    name: PropTypes.string.isRequired,
    onClick: PropTypes.func.isRequired,
    selected: PropTypes.bool.isRequired
};