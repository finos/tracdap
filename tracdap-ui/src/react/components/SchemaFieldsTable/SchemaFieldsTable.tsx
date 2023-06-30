/**
 * A component that shows a schema in a table that can be searched. This component is for viewing in a browser, there is
 * a sister component called {@link SchemaFieldsTablePdf} that is for viewing in a PDF. These two components need to be
 * kept in sync so if a change is made to one then it should be reflected in the other.
 *
 * @module SchemaFieldsTable
 * @category Component
 */

import {applyBooleanFormat} from "../../utils/utils_formats";
import {fillSchemaDefaultValues} from "../../utils/utils_schema";
import PropTypes from "prop-types";
import React, {useCallback, useMemo, useState} from "react";
import {SelectValue} from "../SelectValue";
import type {SelectValuePayload} from "../../../types/types_general";
import {SchemaFieldsRow} from "./SchemaFieldsRow";
import {sortArrayBy} from "../../utils/utils_arrays";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the SchemaFieldsTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The TRAC schema object for the dataset.
     */
    fields?: null | trac.metadata.IFieldSchema[]
}

// The default for the schema prop. This is defined outside the component
// in order to prevent re-renders.
const defaultSchema: Required<Props["fields"]> = []

export const SchemaFieldsTable = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        fields = defaultSchema
    } = props

    // A search term that will filter the table
    const [search, setSearch] = useState<null | string>(null)

    // Sort the schema by the fieldOrder property in case it is not by default
    const sortedFields = useMemo(() =>

        // The fillSchemaDefaultValues function adds in the default values for fieldOrder/categorical/businessKey
        // as these are not transmitted over the wire
        sortArrayBy((fields || []).map(item => fillSchemaDefaultValues(item)), "fieldOrder"), [fields])

    /**
     * A function that filters the schema by the search term entered by the user. The result is memoized so that
     * it only ever runs if either the schema or the search term changes. Null and undefined schema properties
     * are not searchable.
     */
    const filteredFields = useMemo(() =>

        sortedFields.filter((row) => {

                return row && (!search || Object.values(row).some((value) => {

                    // Only fieldOrder is numeric we need to add one to map to the index shown
                    // Booleans also need to be converted to a string to match the input string type of the search
                    const finalValue = typeof value === "number" ? (value + 1).toString() : typeof value === "boolean" ? applyBooleanFormat(value) : value

                    return finalValue != null && finalValue.toUpperCase().includes(search.toUpperCase())
                }))
            }
        ), [sortedFields, search])

    /**
     * A function that runs when the user changes the search box value. The useCallback means that
     * a new function is not created each render.
     */
    const onChangeInput = useCallback((payload: SelectValuePayload<string>) => {

        setSearch(payload.value)

    }, [])

    return (

        <React.Fragment>
            <div className={`${className} d-flex mb-2 align-items-center justify-content-end`}>

                {/*This puts any children on the same row as the search widget but on the left*/}
                {children &&
                    <div className={"flex-fill"}>
                        {children}
                    </div>
                }

                <div className={`${children ? "flex-fill" : "w-100 w-lg-50"}`}>
                    <SelectValue onChange={onChangeInput}
                                 basicType={trac.STRING}
                                 labelPosition={"left"}
                                 labelText={"Search schema:"}
                                 isDispatched={false}
                                 value={search}
                                 validateOnMount={false}
                                 showValidationMessage={false}
                                 mustValidate={false}
                    />
                </div>
            </div>

            <Table className={"mt-3 dataHtmlTable"}>
                <thead>
                <tr>
                    <th>#</th>
                    <th>Field name</th>
                    <th className={"text-center"}>Field type</th>
                    <th>Label</th>
                    <th className={"text-center"}>Format code</th>
                    <th className={"text-center"}>Business key</th>
                    <th className={"text-center"}>Categorical</th>
                    <th className={"text-center"}>Not nullable</th>
                </tr>
                </thead>
                <tbody>

                {filteredFields.map(row =>
                    <SchemaFieldsRow key={row.fieldName} row={row}/>
                )}

                {sortedFields.length === 0 &&
                    <tr>
                        <td colSpan={7} className={"text-center"}>There is no information to display</td>
                    </tr>
                }
                {sortedFields.length > 0 && filteredFields.length === 0 &&
                    <tr>
                        <td colSpan={7} className={"text-center"}>There are no search results</td>
                    </tr>
                }
                </tbody>
            </Table>
        </React.Fragment>
    )
};

SchemaFieldsTable.propTypes = {

    className: PropTypes.string,
    children: PropTypes.oneOfType([
        PropTypes.element,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.element
            ])
        )]),
    fields: PropTypes.arrayOf(PropTypes.shape({
        fieldName: PropTypes.string.isRequired,
        fieldType: PropTypes.number.isRequired,
        label: PropTypes.string,
        fieldOrder: PropTypes.number,
        categorical: PropTypes.bool,
        businessSegment: PropTypes.bool,
        formatCode: PropTypes.string
    }))
};