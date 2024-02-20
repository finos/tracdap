/**
 * A component that shows a button group that has buttons for each basic type available in a
 * dataset schema alongside 'all' and 'none' selectors. This is used to allow users to
 * filter a list of variables by their fieldType.
 *
 * @module FieldTypeSelector
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {arraysOfPrimitiveValuesEqual} from "../utils/utils_arrays";
import type {ButtonPayload, SizeList} from "../../types/types_general";
import {convertBasicTypeToString} from "../utils/utils_trac_metadata";
import {isTracBasicType} from "../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import ToggleButtonGroup from "react-bootstrap/ToggleButtonGroup";
import ToggleButton from "react-bootstrap/ToggleButton";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../config/config_trac_classifications";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the FieldTypeSelector component.
 */
export interface Props {

    /**
     * The css class to apply to the buttons in the group, this allows additional styles to be added to the component.
     */
    buttonClassName?: string,
    /**
     * The function to run when a button is clicked on. This method will not be dispatched.
     */
    dispatchedOnClick?: ActionCreatorWithPayload<ButtonPayload & Record<"fieldTypes", Props["fieldTypes"]>>
    /**
     * The selected field types that the user has selected e.g. STRING.
     */
    fieldTypes: trac.BasicType[]
    /**
     * The id of the selector, used for passing back information about the component.
     */
    id?: string | number
    /**
     * The name of the selector, used for passing back information about the component.
     */
    name?: string
    /**
     * The function to run when a button is clicked on. This method will not be dispatched.
     */
    onClick?: (payload: ButtonPayload & Record<"fieldTypes", Props["fieldTypes"]>) => void
    /**
     * The size of the buttons
     */
    size?: SizeList
}

export const FieldTypeSelector = (props: Props) => {

    console.log("Rendering FieldTypeSelector")

    const {
        buttonClassName = "fs-13 min-width-px-70",
        dispatchedOnClick,
        fieldTypes,
        id,
        name,
        onClick,
        size
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // A list of all the fieldTypes
    const allFieldTypes = Types.tracBasicTypes.map(dataType => dataType.type)

    /**
     * A wrapper function that runs when the user clicks on one of the data type filters. It sets
     * the field type filter to apply to a schema. A wrapper is needed to be able to set the id
     * or property of the store to update.
     *
     * @param newFieldTypes - Array of field types to show.
     * @returns void
     */
    const handleFieldTypeFilterClick = (newFieldTypes: (trac.BasicType | "ALL" | "NONE")[]): void => {

        // This is what we are going to return
        let value: trac.BasicType[] = []

        // If all is selected then we allow all fields to be selected
        if (newFieldTypes.length > 0 && newFieldTypes[0] === "ALL") {

            // Avoid re-renders when all types are already selected
            if (fieldTypes.length === allFieldTypes.length) return
            value = allFieldTypes

        } else if (newFieldTypes.length > 0 && newFieldTypes[0] === "NONE") {

            // Avoid re-renders if when no types are already selected
            if (fieldTypes.length === 0) return

        } else {

            // The different button groups either pass an array of basic types that the user wants to
            // in the variable list or a string, so we have to filter the strings out and pass
            // only an array of basic types to the setFieldTypeButton function
            newFieldTypes.forEach(typeOrString => {
                if (isTracBasicType(typeOrString)) value.push(typeOrString)
            })

            // Avoid re-renders when no selections have changed
            if (arraysOfPrimitiveValuesEqual(fieldTypes, value)) return
        }

        if (onClick) {


            onClick({id, name, fieldTypes: value})

        } else if (dispatchedOnClick) {
            dispatch(dispatchedOnClick({id, name, fieldTypes: value}))
        }
    }

    return (

        <React.Fragment>
            <ToggleButtonGroup className={"mb-2 mb-lg-0 d-lg-inline"}
                               onChange={handleFieldTypeFilterClick}
                               size={size}
                               type={"checkbox"}
                               value={fieldTypes}
            >
                {allFieldTypes.map(fieldType =>
                    <ToggleButton className={buttonClassName}
                                  id={fieldType.toString()}
                                  key={fieldType}
                                  value={fieldType}
                                  variant={"outline-secondary"}>
                        {convertBasicTypeToString(fieldType, false)}
                    </ToggleButton>
                )}

            </ToggleButtonGroup>

            <ToggleButtonGroup className={"ms-0 ms-lg-2 d-none d-lg-inline"}
                               onChange={handleFieldTypeFilterClick}
                               size={size}
                               type={"checkbox"}
                               value={[]}
            >
                <ToggleButton className={buttonClassName}
                              id={"ALL"}
                              value={"ALL"}
                              variant={"outline-secondary"}
                >
                    ALL
                </ToggleButton>
                <ToggleButton className={buttonClassName}
                              id={"NONE"}
                              value={"NONE"}
                              variant={"outline-secondary"}
                >
                    NONE
                </ToggleButton>

            </ToggleButtonGroup>
        </React.Fragment>
    )
};

FieldTypeSelector.propTypes = {

    buttonClassName: PropTypes.string,
    dispatchedOnClick: PropTypes.func,
    fieldTypes: PropTypes.arrayOf(PropTypes.number).isRequired,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    onClick: PropTypes.func
};