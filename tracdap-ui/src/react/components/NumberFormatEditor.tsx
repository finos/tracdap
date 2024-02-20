/**
 * A component that allows a user to edit a format for an integer, float or decimal value. The has
 * options for the use of 1000's separators, decimal places as well as pre- and post-units.
 *
 * @module NumberFormatEditor
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {convertNumberFormatCodeToArray} from "../utils/utils_formats";
import {DecimalPlaces, Sizes, ThousandsSeparators, Units, MultiplierOptions} from "../../config/config_general";
import {isPositionOption} from "../utils/utils_trac_type_chckers";
import type {NumberFormatAsArray, Option, Position, SelectOptionPayload, SelectValuePayload} from "../../types/types_general";
import PropTypes from "prop-types";
import React from "react";
import {SelectOption} from "./SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the NumberFormatEditor component.
 */
export interface Props {

    /**
     * The type of number that the format is for, if no formatCode is set then this is used to assign the default.
     */
    basicType?: trac.BasicType.DECIMAL | trac.BasicType.FLOAT | trac.BasicType.INTEGER
    /**
     * The version of the onChange function to run to update the format when the function need to be dispatched to the redux store.
     */
    dispatchedOnChange?: ActionCreatorWithPayload<SelectValuePayload<string>>
    /**
     * The array version or string version of the format code being edited.
     */
    formatCode: undefined | null | NumberFormatAsArray | string
    /**
     * An identifier for the format that is sent back when the user changes the selected options. Name and ID
     * together allow for two keys to be attached to each format,
     */
    id?: null | string | number
    /**
     * A number index position in an array, usually the position of the field in the schema.
     */
    index?: number
    /**
     * An identifier for the format that is sent back when the user changes the selected options. Name and ID
     * together allow for two keys to be attached to each format,
     */
    name?: null | string
    /**
     * The function to run when the format is edited.
     */
    onChange: (payload: SelectValuePayload<string>) => void,
    /**
     * Whether the edited array is returned as an array or string to the parent component.
     */
    returnAs: "string" | "array"
}

export const NumberFormatEditor = (props: Props) => {

    const {basicType, dispatchedOnChange, formatCode, index, name, onChange, returnAs} = props;

    // This will convert the formatCode if it's not an array version, each part of which is passed to a different select to allow
    // the user to edit. It copes with cases where the formatCode is not defined, in this case the default format for
    // the type of number is assigned.
    const formatCodeAsArray = Array.isArray(formatCode) ? formatCode : convertNumberFormatCodeToArray(formatCode, basicType)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user makes a change to the format of a number, this edits the format array and then
     * updates the store. The payload can be for any of the elements of a format, so some will be for decimal places,
     * units and commas etc.
     * @param index - The index of the formatCode array being edited.
     * @param value - The value to set the new index
     */
    const handleFormatChange = ({id, value}: SelectOptionPayload<Option<string | number, void | Position>, false>): void => {

        // Copy the array
        let newFormatCodeAsArray = [...formatCodeAsArray]

        // Error if the value and index are not of the correct type
        if (typeof id === "number" && value != null) {

            // Handle units that need to be set either pre or post the value
            // If the user has changed the unit
            if (id === 3 && isPositionOption(value)) {

                if (value.details.position === "both") {
                    newFormatCodeAsArray[3] = ""
                    newFormatCodeAsArray[4] = ""
                } else if (value.details.position === "pre") {
                    newFormatCodeAsArray[3] = value.value
                    newFormatCodeAsArray[4] = ""
                } else if (value.details.position === "post") {
                    newFormatCodeAsArray[3] = ""
                    newFormatCodeAsArray[4] = value.value
                }

            } else {
                // Else update the non-unit values
                newFormatCodeAsArray[id] = value.value
            }

            const payload: SelectValuePayload<string> = {
                basicType: trac.STRING,
                id,
                index,
                name,
                value: returnAs === "string" ? newFormatCodeAsArray.join("|") : newFormatCodeAsArray.toString(),
                isValid: true
            }

            dispatchedOnChange ? dispatch(dispatchedOnChange(payload)) : onChange ? onChange(payload) : null

        } else {

            throw new Error("The provided formatCode element in the handleFormatChange function in the NumberFormatEditor component was null")
        }
    }

    // We have to have a bit of extra logic as the format code units can be either in index 3 or 4 depending on
    // whether they are set pre or post the value e.g. £ or % are shown differently
    const unitsOptionToSearchFor = formatCodeAsArray[3] === "" && formatCodeAsArray[4] === "" ? "" : formatCodeAsArray[3] === "" && formatCodeAsArray[4] !== "" ? formatCodeAsArray[4] : formatCodeAsArray[3]

    return (
        // Note that this component outer div has the 'flex-fill' class,  this component must be placed within a parent div that has the d-flex class
        <div className={"flex-fill"}>
            <div className={"d-flex flex-row align-items-end"}>

                <SelectOption basicType={trac.STRING}
                              className={"flex-fill"}
                              id={3}
                              isDispatched={false}
                              labelText={"Units"}
                              onChange={handleFormatChange}
                              options={Units}
                              validateOnMount={false}
                    // The find for "" returns the "None" default option, if the user selects 'billion" as the size then this goes into the index 4 position
                    // but 'billion" is not a unit option so a single find against the value will return undefined
                              value={Units.find(unit => unit.value === unitsOptionToSearchFor) || Units.find(unit => unit.value === "")}
                />

                <span className={"mt-auto mx-1 mb-2"}>&#8211;</span>

                <SelectOption basicType={trac.INTEGER}
                              className={"flex-fill"}
                              id={2}
                              isDispatched={false}
                              labelText={"Decimal places"}
                              onChange={handleFormatChange}
                              options={DecimalPlaces}
                              validateOnMount={false}
                              value={DecimalPlaces.find(dp => dp.value === formatCodeAsArray[2])}
                />

                <span className={"d-none d-md-block mt-auto mx-1 mb-2"}>&#8211;</span>

                <SelectOption basicType={trac.STRING}
                              className={"flex-fill"}
                              id={4}
                              isDispatched={false}
                              labelText={"Size"}
                              onChange={handleFormatChange}
                              options={Sizes}
                              validateOnMount={false}
                    // The find for "" returns the "None" default option, if the user selects "%" as the units then this goes into the index 4 position
                    // but "%" is not a size option so a single find against the value will return undefined
                              value={Sizes.find(size => size.value === formatCodeAsArray[4]) || Sizes.find(size => size.value === "")}
                />

                <span className={"mt-auto mx-1 mb-2"}>&#8211;</span>

                <SelectOption basicType={trac.STRING}
                              className={"flex-fill"}
                              id={0}
                              isDispatched={false}
                              labelText={"000s separator"}
                              onChange={handleFormatChange}
                              options={ThousandsSeparators}
                              validateOnMount={false}
                              value={ThousandsSeparators.find(thousandsSeparator => thousandsSeparator.value === formatCodeAsArray[0])}
                />

                <span className={"mt-auto mx-1 mb-2"}>&#8211;</span>

                <SelectOption basicType={trac.INTEGER}
                              className={"flex-fill"}
                              id={5}
                              isDispatched={false}
                              labelText={"Multiplier"}
                              onChange={handleFormatChange}
                              options={MultiplierOptions}
                              validateOnMount={false}
                              tooltip={"This is the multiplier to apply to the raw number to make the units correct e.g. if the number is 0.01 for 1% the multiplier should be 100"}
                              value={MultiplierOptions.find(multiplier => multiplier.value === formatCodeAsArray[5])}
                />

            </div>
        </div>
    )
};

NumberFormatEditor.propTypes = {

    basicType: PropTypes.number,
    dispatchedOnChange: PropTypes.func,
    formatCode: PropTypes.oneOfType([PropTypes.string, PropTypes.array]),
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    name: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    onChange: PropTypes.func,
    returnAs: PropTypes.string.isRequired
};