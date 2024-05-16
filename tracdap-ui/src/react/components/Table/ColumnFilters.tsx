/**
 * A component that shows a widget to use to filter a column in a table, for example for string columns this can either
 * be a case-insensitive search or a select from a list for categorical variables, for numbers it can be a min/max filter.
 *
 * @module ColumnFilters
 * @category Component
 */

import type {Column, Table as TableType} from "@tanstack/react-table";
import {convertArrayToOptions, makeArrayOfObjectsUniqueByProperty} from "../../utils/utils_arrays";
import {isDateFormat, isOption, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import type {Option, SelectDatePayload, SelectOptionPayload, SelectValuePayload, TableRow} from "../../../types/types_general";
import React from "react";
import {SelectDate} from "../SelectDate";
import {SelectOption} from "../SelectOption"
import {SelectValue} from "../SelectValue";

/**
 * An interface for the props of the ColumnFilters component.
 */
export interface Props {

    column: Column<TableRow>
    table: TableType<TableRow>
}

export const ColumnFilters = (props: Props) => {

    const {column, column: {getFacetedUniqueValues}, table} = props

    // Get the field type for the column, this is stored in the column meta property
    const fieldType = column.columnDef.meta?.schema.fieldType
    // Get whether the column is categorical, this is stored in the column meta property
    const categorical = column.columnDef.meta?.schema.categorical
    // Get whether the column has a format code, this is stored in the column meta property and used for dates
    const formatCode = column.columnDef.meta?.schema.formatCode

    // Get the value of the filter to apply to the column, this is stored in the table state for the column.
    const columnFilterValue = column.getFilterValue()
    const columnFilters = table.getState().columnFilters

    // For categorical strings we need to show a list of options, so we get a list of the unique values
    const sortedUniqueOptions = React.useMemo(() => {

        // We need to regenerate the options when filters are applied, to achieve this we add the columnFilters
        // in the table state as a dependency, but to prevent a warning in pyCharm we have to spoof a use of the variable
        if (!(columnFilters && ((isTracString(fieldType) && categorical) || isTracBoolean(fieldType)))) {

            return []

        } else {

            // If the column values are arrays we want to sort each array so that we deduplicate items that have
            // the same items in the list but in a different order e.g. [A, B] and [B, A] are seen as the same.
            // We also remove null values so these are searches that can not be done.
            let listOfValues = [...getFacetedUniqueValues().keys()].map(item => Array.isArray(item) ? [...item].sort() : item)

            // Is a null value in the data, if yes then we will add a null option to search the table by, but we want this to be
            // at the top of the list of options
            const hasNull = listOfValues.some(value => value === null)

            // The null option to add at the top
            const nullOption = hasNull ? convertArrayToOptions([null]) : []

            // Remove all null values if we found one
            if (hasNull) {
                listOfValues = listOfValues.filter(value => value !== null)
            }

            // Never allow more than 1000 options
            return nullOption.concat(makeArrayOfObjectsUniqueByProperty(convertArrayToOptions([...new Set(listOfValues.sort())], false).slice(0, 1000), "label"))
        }

    }, [fieldType, categorical, getFacetedUniqueValues, columnFilters])

    return isTracNumber(fieldType) ? (

        <React.Fragment>
            <SelectValue basicType={fieldType}
                         className={"mb-1"}
                         isDispatched={false}
                         onChange={(payload: SelectValuePayload) => column.setFilterValue((old: [number, number]) => [payload.value, old?.[1]])}
                         maximumValue={Number(column.getFacetedMinMaxValues()?.[1] ?? '')}
                         minimumValue={Number(column.getFacetedMinMaxValues()?.[0] ?? '')}
                         mustValidate={false}
                         placeHolderText={"Min"}
                         showValidationMessage={false}
                         size={"sm"}
                         validateOnMount={false}
                         value={(columnFilterValue as [number, number])?.[0] ?? null}

            />
            <SelectValue basicType={fieldType}

                         isDispatched={false}
                         onChange={(payload: SelectValuePayload) => column.setFilterValue((old: [number, number]) => [old?.[0], payload.value])}
                         maximumValue={Number(column.getFacetedMinMaxValues()?.[1] ?? '')}
                         minimumValue={Number(column.getFacetedMinMaxValues()?.[0] ?? '')}
                         mustValidate={false}
                         placeHolderText={"Max"}
                         showValidationMessage={false}
                         size={"sm"}
                         validateOnMount={false}
                         value={(columnFilterValue as [number, number])?.[1] ?? null}
            />
        </React.Fragment>

    ) : isTracBoolean(fieldType) ? (

        // Note that we attach the dropdown outside the table in the DOM using the 'useMenuPortalTarget' prop, this is
        // so it is not clipped by the table boundary
        <SelectOption basicType={fieldType}
                      hideDropdown={false}
                      isClearable={false}
                      isDispatched={false}
                      mustValidate={false}
                      options={sortedUniqueOptions}
                      onChange={(payload: SelectOptionPayload<Option<string>, false>) => column.setFilterValue(payload.value)}
                      placeHolderText={`Select`}
                      showValidationMessage={false}
                      size={"sm"}
                      useMenuPortalTarget={true}
                      validateOnMount={false}
                      value={isOption(columnFilterValue) ? columnFilterValue : null}
        />

    ) : (isTracString(fieldType) && column.columnDef.meta?.schema.categorical) ? (

        // Note that we attach the dropdown outside the table in the DOM using the 'useMenuPortalTarget' prop, this is
        // so it is not clipped by the table boundary
        <SelectOption basicType={fieldType}
                      hideDropdown={false}
                      isClearable={false}
                      isDispatched={false}
                      mustValidate={false}
                      options={sortedUniqueOptions}
                      onChange={(payload: SelectOptionPayload<Option<string>, false>) => column.setFilterValue(payload.value)}
                      placeHolderText={sortedUniqueOptions.length > 0 ? `Select (${sortedUniqueOptions.length})` : "No options"}
                      showValidationMessage={false}
                      size={"sm"}
                      useMenuPortalTarget={true}
                      validateOnMount={false}
                      value={isOption(columnFilterValue) ? columnFilterValue : null}
        />

    ) : (isTracString(fieldType) && !column.columnDef.meta?.schema.categorical) ? (

        <SelectValue basicType={fieldType}
                     isDispatched={false}
                     maximumValue={Number(column.getFacetedMinMaxValues()?.[1] ?? '')}
                     minimumValue={Number(column.getFacetedMinMaxValues()?.[0] ?? '')}
                     mustValidate={false}
                     onChange={(payload: SelectValuePayload) => column.setFilterValue(payload.value || "")}
                     placeHolderText={"Search"}
                     showValidationMessage={false}
                     size={"sm"}
                     validateOnMount={false}
                     value={columnFilterValue as string || null}
        />

    ) : isTracDateOrDatetime(fieldType) ?

        <React.Fragment>
            <SelectDate basicType={fieldType}
                        className={"mb-1"}
                        formatCode={isDateFormat(formatCode) ? formatCode : null}
                        isDispatched={false}
                        mustValidate={false}
                        onChange={(payload: SelectDatePayload) => column.setFilterValue((old: [string | null, string | null]) => {
                            return !old?.[1] && !payload.value ? undefined : [payload.value || null, old?.[1]]
                        })}
                        placeHolderText={"Min"}
                        position={"start"}
                        showValidationMessage={false}
                        size={"sm"}
                        useMenuPortalTarget={true}
                        validateOnMount={false}
                        value={(columnFilterValue as [string, string])?.[0] ?? null}
            />
            <SelectDate basicType={fieldType}
                        formatCode={isDateFormat(formatCode) ? formatCode : null}
                        isDispatched={false}
                        mustValidate={false}
                        onChange={(payload: SelectDatePayload) => column.setFilterValue((old: [string | null, string | null]) => {
                            return !old?.[0] && !payload.value ? undefined : [old?.[0], payload.value || null]
                        })}
                        placeHolderText={"Max"}
                        position={"end"}
                        showValidationMessage={false}
                        size={"sm"}
                        useMenuPortalTarget={true}
                        validateOnMount={false}
                        value={(columnFilterValue as [string, string])?.[1] ?? null}
            />
        </React.Fragment>

        : null
};