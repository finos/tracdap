/**
 * A component that shows a regular row in a table, we have a separate row component to memoize it which allows for less
 * re-rendering.
 *
 * @module Row
 * @category Component
 */

import {type Cell, flexRender} from "@tanstack/react-table";
import {getHeatmapColour, getTrafficLightColour} from "../../utils/utils_general";
import {hasOwnProperty, isTracString} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {memo} from "react";
import type {ColumnColourDefinitions, TableRow, TableValues} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Row component.
 */
export interface Props {

    /**
     * An object containing information about what columns have heat maps or traffic light colouring set and the
     * information required to set the colours.
     */
    columnColours: ColumnColourDefinitions
    /**
     * A function that returns a class to align the text in the cell based on the TRAC type of the column.
     */
    getTextAlignment: (basicType: null | undefined | trac.BasicType) => string
    /**
     * Function that when ran provides the function to run when the row is clicked on that toggles its selected state.
     */
    getToggleSelectedHandler: () => (event: unknown) => void
    /**
     * The row number, used to set the row height in the react-table plugin.
     */
    index: number,
    /**
     * Whether the row is selected.
     */
    isSelected: boolean
    /**
     * A ref for the row that is passed from the react-virtual plugin to get the measurements of the row when they are rendered. This
     * is needed because the rows have dynamic heights and without telling the react-virtual hook what the actual height of the rows
     * is the scroll bar and the table get out of sync.
     */
    measureElement?: React.LegacyRef<HTMLTableRowElement>
    /**
     * Array of column IDs that should not have their string values wrapped.
     */
    noWrapColumns: string[]
    /**
     * The visible cells for the row.
     */
    visibleCells: Cell<TableRow, unknown>[]
}

const RowInner = (props: Props) => {

    const {
        columnColours,
        getTextAlignment,
        getToggleSelectedHandler,
        index,
        isSelected,
        measureElement,
        noWrapColumns,
        visibleCells
    } = props

    const getColumnColour = (id: string, columnColours: ColumnColourDefinitions, value: TableValues): undefined | { background: undefined | string, color: undefined | string } => {

        if (!hasOwnProperty(columnColours, id)) return undefined

        if (columnColours[id].type === "heatmap") {

            return getHeatmapColour(columnColours[id], value as TableValues)

        } else if (columnColours[id].type === "trafficlight") {

            return getTrafficLightColour(columnColours[id], value as TableValues)

        } else {

            return undefined
        }
    }

    return (
        <tr onClick={getToggleSelectedHandler()}
            className={`${isSelected ? "selected" : ""}`}
            ref={measureElement}
            data-index={index}
        >
            {visibleCells.map(cell => {

                // Whether the text in cell can be wrapped
                const noWrapClassName = isTracString(cell.column.columnDef.meta?.schema.fieldType) && !noWrapColumns.includes(cell.column.id) ? "" : "text-nowrap"
                // WHat is the alignment of the text
                const textAlignmentClassName = getTextAlignment(cell.column.columnDef.meta?.schema.fieldType)

                const heatMapColour = !isSelected ? getColumnColour(cell.column.id, columnColours, cell.getValue<TableValues>()) : undefined

                return (
                    <td key={cell.id}
                        className={`${noWrapClassName} ${textAlignmentClassName}`}
                        style={heatMapColour}
                    >
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                )
            })}
        </tr>
    )
};

RowInner.propTypes = {

    getTextAlignment: PropTypes.func.isRequired,
    getToggleSelectedHandler: PropTypes.func.isRequired,
    isSelected: PropTypes.bool,
    noWrapColumns: PropTypes.arrayOf(PropTypes.string),
    visibleCells: PropTypes.arrayOf(PropTypes.object).isRequired
};

export const Row = memo(RowInner);