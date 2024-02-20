/**
 * A component that allows the user to set a field in the schema editor as categorical. Not all variable types can be categorical.
 *
 * @module Categorical
 * @category Component
 */

import {canBeCategorical} from "../../utils/utils_general";
import PropTypes from "prop-types";
import React from "react";
import {SelectToggle} from "../SelectToggle";
import type {SelectTogglePayload} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Categorical component.
 */
export interface Props {

    /**
     * Whether the fieldName can be edited. When creating a schema you want to be able to edit it but not when
     * editing the schema of a dataset loaded by CSV or Excel.
     */
    canEditFieldName: boolean
    /**
     * Whether the field is categorical.
     */
    categorical: trac.metadata.IFieldSchema["categorical"]
    /**
     * The name of the field in the schema.
     */
    fieldName: trac.metadata.IFieldSchema["fieldName"]
    /**
     * The name of the field in the schema.
     */
    fieldType: trac.metadata.IFieldSchema["fieldType"]
    /**
     * The position of the field in the schema.
     */
    index: number
    /**
     * The function to run when the field schema is changed. This updates the edited property in the parent.
     * This will either be in a store or a state of the parent component.
     */
    onChange: (payload: SelectTogglePayload) => void
}

export const Categorical = (props: Props) => {

    const {
        canEditFieldName,
        categorical,
        fieldName,
        fieldType,
        index,
        onChange
    } = props

    return (
        <div className={"d-flex align-items-end justify-content-center flex-shrink-0 ps-3 pe-2"}>
            <SelectToggle className={"d-block text-center"}
                          id={fieldName}
                          index={index}
                          isDisabled={!canBeCategorical(fieldType)}
                          isDispatched={false}
                          labelText={canEditFieldName ? "Categorical:" : undefined}
                          labelPosition={"top"}
                          name={"categorical"}
                          onChange={onChange}
                          tooltip={"Categorical fields have discrete groups of information. Only string fields can be categorical."}
                          validateOnMount={false}
                          value={categorical || null}
            />
        </div>
    )
};

Categorical.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    categorical: PropTypes.bool,
    fieldName: PropTypes.string.isRequired,
    fieldType: PropTypes.number.isRequired,
    index: PropTypes.number.isRequired,
    onChange: PropTypes.func.isRequired
};