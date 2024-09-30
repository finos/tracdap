/**
 * A component that allows the user to set a field type for a field in the schema editor e.g. BOOLEAN.
 *
 * @module FieldOrder
 * @category Component
 */

import type {Option, SelectOptionPayload} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";

/**
 * A function that takes the recommended data types from the guessVariableTypes function and
 * filters the list of TRAC data types to just those that we guessed that the data can be.
 * We always add in STRING as it could be that the data is a number that should be stored
 * as a string.
 *
 * @param recommendedDataTypes - A array of recommended data types for a column in a dataset.
 * @param tracBasicTypes - A set of options for all data types in TRAC.
 * @returns - A set of options for the user to set the data type for their data.
 */
function setFieldTypeOptions(recommendedDataTypes: trac.BasicType[], tracBasicTypes: Option<string, trac.BasicType>[]) {

    return tracBasicTypes.filter(option => recommendedDataTypes.includes(option.type))
}

/**
 * An interface for the props of the FieldType component.
 */
export interface Props {

    /**
     * Whether the fieldName can be edited. When creating a schema you want to be able to edit it but not when
     * editing the schema of a dataset loaded by CSV or Excel.
     */
    canEditFieldName: boolean
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
    onChange: (payload: SelectOptionPayload<Option<string>, false>) => void
    /**
     * The types that the guessVariableTypes util function says will work with the data.
     */
    recommendedDataTypes: trac.BasicType[]
}

export const FieldType = (props: Props) => {

    const {
        canEditFieldName,
        fieldName,
        fieldType,
        index,
        onChange,
        recommendedDataTypes
    } = props

    // We memoized the variables below because they are arrays, if we do not then a change to a schema for a variable
    // will cause the entire SelectOption to rerender because React sees the arrays as being a different object. Rendering is
    // quick so ordinarily this is not an issue, here however there will also be a rerender of the table, so we are
    // trying to optimise the rendering to make it less of an overhead

    // A set of options for TRAC basic types, limited to those that will work with the data
    const fieldTypeOptions = useMemo(() => setFieldTypeOptions(recommendedDataTypes, Types.tracBasicTypes), [recommendedDataTypes])

    // The selected fieldType option
    const fieldTypeValue = useMemo(() => fieldTypeOptions.find(dataType => dataType.type === fieldType), [fieldType, fieldTypeOptions])

    return (

        <div className={"d-flex align-items-end flex-fill flex-lg-grow-0 w-lg-10 px-2"}>
            <SelectOption basicType={trac.STRING}
                          className={"w-100"}
                          id={fieldName}
                          index={index}
                          labelText={canEditFieldName ? "Type:" : undefined}
                          labelPosition={"top"}
                          name={"fieldType"}
                          onChange={onChange}
                          options={fieldTypeOptions}
                          validateOnMount={false}
                          value={fieldTypeValue || null}
                          isDispatched={false}
            />
        </div>
    )
}

FieldType.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    fieldName: PropTypes.string.isRequired,
    fieldType: PropTypes.number.isRequired,
    guessedVariableTypes: PropTypes.shape({
        types: PropTypes.shape({
            found: PropTypes.arrayOf(PropTypes.string),
            recommended: PropTypes.arrayOf(PropTypes.number)
        }),
        inFormats: PropTypes.shape({found: PropTypes.arrayOf(PropTypes.string)})
    }),
    index: PropTypes.number.isRequired,
    onChange: PropTypes.func.isRequired
};