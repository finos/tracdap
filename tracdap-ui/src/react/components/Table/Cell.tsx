/**
 * A component that shows the contents of a cell in the table. This is not memoized.
 *
 * @module Cell
 * @category Component
 */

import {Badges} from "../Badges";
import {type CellContext} from "@tanstack/react-table";
import {isDateObject, isTracString} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useState} from "react";
import {setAnyFormat} from "../../utils/utils_formats";
import type {TableRow, TableValues, Variants} from "../../../types/types_general";

/**
 * An interface for the props of the Cell component.
 */
export interface Props {

    /**
     * A lookup for the values in the cell and the colour that they should be shown in if they should be
     * treated as badges. This means that we can have red amber green badges based on their value. If other colourings
     * for specific values are needed then these can be set via this prop.
     */
    badgeColours?: Record<string, Variants>
    /**
     * The cell item context, this is provided by the react-table plugin.
     */
    info: CellContext<TableRow, TableValues>
    /**
     * String values greater than this length with be truncated, the user can click to show the full text. XXX unwrap anything that is selected XXX
     */
    truncateTextLimit: number
}

export const Cell = (props: Props) => {

    const {
        badgeColours,
        info,
        truncateTextLimit
    } = props

    const id = info.cell.column.id

    // Whether a string value should be truncated or not if it is very long
    const [truncateText, setTruncateText] = useState<boolean>(true)

    const value = info.getValue()

    // So now we know that the column is visible and there is a filter value to apply, so we need to do a little
    // work. Get the column details, this contains the meta we stored in it and the column visibility
    const schema = info.table.getColumn(id)?.columnDef.meta?.schema

    // Some strings we want to show in badges
    const treatAsArray = info.table.getColumn(id)?.columnDef.meta?.treatAsArray

    // If there is no schema we error, this is a required prop
    if (!schema || !schema.fieldType) throw new Error(`Column ${id} does not have a schema or fieldType set, this is not allowed.`)

    // First deal with string arrays or strings that we want to handle like they are arrays and
    // show them as badges. The badgeColours object can be used to set the colour to show
    // based on the value of the strings.

    if (value != null && Array.isArray(value)) {

        // Make arrays of the same values but different orders always appear the same way
        // We do not set the variant using badgeColours with arrays
        return <Badges convertText={true} text={value.slice().sort((a, b) => a.localeCompare(b))}/>

    } else if (value != null && treatAsArray) {

        // Boolean and dates values can't be used as the keys of objects
        return <Badges convertText={true}
                       text={value.toString()}
                       variant={typeof (value) === "boolean" || !badgeColours  || isDateObject(value) ? undefined : badgeColours[value]}/>

    } else if (typeof value === "string" && isTracString(schema.fieldType) && value.length > truncateTextLimit) {

        // Now we deal with string variables, these are truncated if they are really long but can be un-truncated if
        // clicked on. Selection onClick events screws with the onClick event specified here so if a row is
        // selected nothing is truncated.
        return <span className={"pointer"}
                     onClick={() => !info.row.getIsSelected() ? setTruncateText(!truncateText) : undefined}>
                     {truncateText && !info.row.getIsSelected() ? `${value.slice(0, truncateTextLimit)}...` : value}
                    </span>

    } else {

        // Numbers, booleans, date and datetime
        return <span>{setAnyFormat(schema.fieldType, schema.formatCode || null, value)}</span>
    }
};

Cell.propTypes = {
    info: PropTypes.object.isRequired,
    truncateTextLimit: PropTypes.number.isRequired
};