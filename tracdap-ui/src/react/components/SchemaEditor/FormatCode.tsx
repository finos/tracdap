/**
 * A component that allows the user to set a format codr for a field in the schema editor.
 * Not all variable types can have format codes set.
 *
 * @module FormatCode
 * @category Component
 */

import {convertObjectKeysToOptions} from "../../utils/utils_object";
import {DateFormats} from "../../../config/config_general";
import {isTracDateOrDatetime, isTracNumber} from "../../utils/utils_trac_type_chckers";
import {NumberFormatEditor} from "../NumberFormatEditor";
import type {Option, SelectPayload} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {memo, useMemo} from "react";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the FormatCode component.
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
     * The format code of the field in the schema.
     */
    formatCode: trac.metadata.IFieldSchema["formatCode"]
    /**
     * The position of the field in the schema.
     */
    index: number
    /**
     * The function to run when the field schema is changed. This updates the edited property in the parent.
     * This will either be in a store or a state of the parent component.
     */
    onChange: (payload: SelectPayload<Option<string>, false>) => void
}

export const FormatCodeInner = (props: Props) => {

    const {
        canEditFieldName,
        fieldName,
        fieldType,
        formatCode,
        index,
        onChange
    } = props

    // We memoized the variables below because they are arrays, if we do not then a change to a schema for a variable
    // will cause the entire row to rerender because React sees the arrays as being a different object. Rendering is
    // quick so ordinarily this is not an issue, here however there will also be a rerender of the table, so we are
    // trying to optimise the rendering to make it less of an overhead

    // A set of options for each of the date and datetime formats
    const dateFormatOptions = useMemo(() => convertObjectKeysToOptions(DateFormats, true), [])

    // The selected formatCode option (date and datetime only)
    const formatCodeValue = useMemo(() => dateFormatOptions.find(dateFormatOption => (dateFormatOption.value === null && formatCode === null) || (formatCode && dateFormatOption.value === formatCode.toUpperCase())), [dateFormatOptions, formatCode])

    return (

        <div className={`${!isTracNumber(fieldType) ? "d-flex" : ""} flex-fill align-items-end ps-1 pe-1 px-lg-2`}>
            {isTracNumber(fieldType) &&
                <NumberFormatEditor basicType={fieldType}
                                    formatCode={formatCode}
                                    id={fieldName}
                                    index={index}
                                    name={"formatCode"}
                                    onChange={onChange}
                                    returnAs={"string"}
                />
            }

            {isTracDateOrDatetime(fieldType) &&
                <SelectOption basicType={trac.STRING}
                              className={"w-100 w-md-50 w-lg-50 pe-2 pe-lg-0"}
                              id={fieldName}
                              index={index}
                              labelText={canEditFieldName ? "Format:" : undefined}
                              labelPosition={"top"}
                              name={"formatCode"}
                              onChange={onChange}
                              isDispatched={false}
                              options={dateFormatOptions.filter(dateFormatOption => fieldType === trac.DATETIME || !["FILENAME", "DATETIME", "TIME"].includes(dateFormatOption.value))}
                              validateOnMount={false}
                              value={formatCodeValue}
                />
            }

            {fieldType && [trac.BOOLEAN, trac.STRING].includes(fieldType) && !canEditFieldName &&
                <span className={"my-auto d-none d-lg-block"}>Format not applicable</span>
            }

        </div>
    )
};

FormatCodeInner.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    fieldName: PropTypes.string.isRequired,
    fieldType: PropTypes.number.isRequired,
    formatCode: PropTypes.string,
    index: PropTypes.number.isRequired,
    onChange: PropTypes.func.isRequired,
};

export const FormatCode =  memo(FormatCodeInner);