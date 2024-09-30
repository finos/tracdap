/**
 * A component that shows a row in the SchemaFieldsRow component.
 *
 * @module SchemaFieldsRow
 * @category Component
 */

import {applyBooleanFormat} from "../../utils/utils_formats";
import {convertBasicTypeToString} from "../../utils/utils_trac_metadata";
import React, {memo} from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the SchemaFieldsRow component.
 */
export interface Props {

    row: trac.metadata.IFieldSchema
}

const TableRowInner = (props: Props) => {

    const {row: {fieldOrder, fieldName, fieldType, businessKey, categorical, label, formatCode, notNull}} = props

    return (
        <tr>
            <td>{fieldOrder != null ? fieldOrder + 1 : "Not set"}</td>
            <td>{fieldName || "Not set"}</td>
            <td className={"text-center"}>{fieldType != null ? convertBasicTypeToString(fieldType) : "Not set"}</td>
            <td>{label || "Not set"}</td>
            <td className={"text-center"}>{fieldType && [trac.STRING, trac.BOOLEAN].includes(fieldType) ? "-" : !formatCode ? "Not set" : formatCode}</td>
            <td className={"text-center"}>{businessKey != null ? applyBooleanFormat(businessKey) : "Not set"}</td>
            <td className={"text-center"}>{categorical != null ? applyBooleanFormat(categorical) : "Not set"}</td>
            <td className={"text-center"}>{notNull != null ? applyBooleanFormat(notNull) : "Not set"}</td>
        </tr>
    )
};

export const SchemaFieldsRow = memo(TableRowInner, (prevProps, nextProps) => prevProps.row.fieldName === nextProps.row.fieldName);