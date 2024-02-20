/**
 * A component that shows a menu that allows the user to edit the schema properties of a field in a schema.
 * Note that the individual items that make up the field schema definition are passed as primitive types so
 * that we can memoize the row and limit the re-renders. If we passed an object then when the schema is
 * updated in the store we may cause all the SchemaRow components to render.
 *
 * @module SchemaRow
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import type {ButtonPayload, Option, SelectPayload, UpdateSchemaPayload} from "../../../types/types_general";
import {canBeCategorical} from "../../utils/utils_general";
import {Categorical} from "./Categorical";
import {FieldName} from "./FieldName";
import {FieldOrder} from "./FieldOrder";
import {FieldType} from "./FieldType";
import {FormatCode} from "./FormatCode";
import {isObject, isOption, isTracDateOrDatetime, isTracNumber, isTypeOption} from "../../utils/utils_trac_type_chckers";
import {Label} from "./Label";
import {objectsEqual} from "../../utils/utils_object";
import PropTypes from "prop-types";
import React, {memo, useCallback} from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../../types/types_hooks";

/**
 * An interface for the props of the SchemaRow component.
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
     * The function to run to update the edited schema in the store.
     */
    dispatchedUpdateSchema: ActionCreatorWithPayload<UpdateSchemaPayload>
    /**
     * The format code of the field in the schema.
     */
    formatCode: trac.metadata.IFieldSchema["formatCode"]
    /**
     * The name of the field in the schema.
     */
    fieldName: trac.metadata.IFieldSchema["fieldName"]
    /**
     * The order of the field in the schema.
     */
    fieldOrder: trac.metadata.IFieldSchema["fieldOrder"]
    /**
     * The name of the field in the schema.
     */
    fieldType: trac.metadata.IFieldSchema["fieldType"]
    /**
     * The position of the field in the schema.
     */
    index: number
    /**
     * The name of the field in the schema.
     */
    label: trac.metadata.IFieldSchema["label"]
    /**
     * The types that the guessVariableTypes util function says will work with the data.
     */
    recommendedDataTypes: trac.BasicType[]
    /**
     * The number of fields in the schema, this is used to disable some buttons.
     */
    schemaLength: number
}

const SchemaRowInner = (props: Props) => {

    console.log("Rendering SchemaRow")

    const {
        canEditFieldName,
        categorical,
        dispatchedUpdateSchema,
        fieldName,
        fieldOrder,
        fieldType,
        formatCode,
        index,
        label,
        recommendedDataTypes,
        schemaLength
    } = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that apples the schema change and then returns the updated field schema. This function is here rather than
     * in a store because all use cases of the SchemaEditor component will require this update.
     */
    const onNonFieldOrderChange = useCallback((payload: SelectPayload<Option<string, void | trac.BasicType>, false>): void => {

        const {name, index: payloadIndex} = payload

        if (payloadIndex === undefined) return

        const originalSchema = {categorical, fieldName, fieldOrder, fieldType, formatCode, label}
        let newSchema: trac.metadata.IFieldSchema = {...originalSchema}

        // Get the value to set, this needs to take account of the fact that the payload could come from
        // any of the Select components by they have different payloads
        const value = isObject(payload) && isOption(payload?.value) ? payload.value.value : payload.value

        // Handle updates to the label
        if (name === "label" && (value === null || typeof value === "string")) {

            newSchema[name] = value

        } else if (name === "fieldName" && (value === null || typeof value === "string")) {

            newSchema[name] = value

        } else if (name === "fieldType" && isTypeOption(payload.value)) {

            newSchema[name] = payload.value.type

            // Only numbers and dates can be formatted
            if (!(isTracNumber(payload.value.type) || isTracDateOrDatetime(payload.value.type))) {
                newSchema["formatCode"] = null
            }

            // Only strings can be categorical
            if (!canBeCategorical(payload.value.type)) {
                newSchema["categorical"] = false
            }

        } else if (name === "formatCode") {

            if (isOption(payload.value)) {
                newSchema[name] = payload.value.value
            } else if (payload.value == null || typeof payload.value === "string") {
                newSchema[name] = payload.value
            }

        } else if ((name === "categorical" || name === "businessKey") && (value === null || typeof value === "boolean")) {

            newSchema[name] = value
        }

        // This function is expensive to call as it re-renders the table and the schema editor. We have put a debouncing function
        // on the Label component to reduce re-renders, however that causes a useEffect to make additional function calls. In order
        // to make sure that no unnecessary calls are made we make additional checks her
        if (!objectsEqual(newSchema, originalSchema)) {
            console.log("LOG :: Expensive re-render happening")
            dispatch(dispatchedUpdateSchema({index: payloadIndex, fieldSchema: newSchema}))
        }

    }, [categorical, dispatch, dispatchedUpdateSchema, fieldName, fieldOrder, fieldType, formatCode, label])

    /**
     * A function that applies the schema change for a change in order and then returns the updated field schema.
     * This function is here rather than in a store because all use cases of the SchemaEditor component will
     * require this update. A separate function is needed for an order change in order to avoid a large amount
     * of Typescript checks to handle all the payload interfaces.
     */
    const onFieldOrderChange = useCallback((payload: ButtonPayload): void => {

        const {name, index: payloadIndex} = payload

        if (payloadIndex === undefined) return

        let newSchema: trac.metadata.IFieldSchema = {categorical, fieldName, fieldOrder, fieldType, formatCode, label}

        if (name === "up" || name === "down") {

            // Handle changes to fieldOrder which will either be up or down
            const oldFieldOrder = newSchema.fieldOrder

            if (oldFieldOrder != null) {

                // If moving up the list the fieldOrder will go down by one
                const upOrDownStep = name === "up" ? -1 : +1

                // If the fieldOrder is going down/up by one then we need to find the variable that is in the position already
                const fieldOrderToSwapWith = oldFieldOrder + upOrDownStep

                // Now swap the fieldOrders
                newSchema["fieldOrder"] = oldFieldOrder + upOrDownStep

                dispatch(dispatchedUpdateSchema({index: payloadIndex, fieldSchema: newSchema, fieldOrder: {fieldOrderToSwapWith, oldFieldOrder}}))

            } else {
                throw new TypeError(`The oldFieldOrder index was null, this is not allowed`)
            }
        }

    }, [categorical, dispatch, dispatchedUpdateSchema, fieldName, fieldOrder, fieldType, formatCode, label])

    return (

        // The child components have the props passed as primitive values so that they can be memoized.
        // Note that the classes allow for two layouts, large and small screens have different views
        <div className={"schema-editor-item py-3 px-2 flex-column flex-fill"}>

            <div className={`flex-fill ${canEditFieldName ? "" : "pb-2"}`}>
                <FieldName canEditFieldName={canEditFieldName}
                           fieldName={fieldName}
                           index={index}
                           onChange={onNonFieldOrderChange}
                />
            </div>

            {/*Hide on small screens*/}
            <div className={"d-none d-lg-flex flex-row"}>
                <Label canEditFieldName={canEditFieldName}
                       fieldName={fieldName}
                       index={index}
                       label={label}
                       onChange={onNonFieldOrderChange}
                />

                <FieldType canEditFieldName={canEditFieldName}
                           fieldName={fieldName}
                           fieldType={fieldType}
                           index={index}
                           onChange={onNonFieldOrderChange}
                           recommendedDataTypes={recommendedDataTypes}
                />


                <FormatCode canEditFieldName={canEditFieldName}
                            fieldName={fieldName}
                            fieldType={fieldType}
                            formatCode={formatCode}
                            index={index}
                            onChange={onNonFieldOrderChange}/>

                <FieldOrder canEditFieldName={canEditFieldName}
                            fieldName={fieldName}
                            fieldOrder={fieldOrder}
                            index={index}
                            onChange={onFieldOrderChange}
                            schemaLength={schemaLength}
                />

                <Categorical canEditFieldName={canEditFieldName}
                             categorical={categorical}
                             fieldName={fieldName}
                             fieldType={fieldType}
                             index={index}
                             onChange={onNonFieldOrderChange}
                />
            </div>

            {/*Hide on large screens*/}
            <div className={"d-flex d-lg-none flex-row pb-2 pb-lg-0"}>
                <Label canEditFieldName={canEditFieldName}
                       fieldName={fieldName}
                       index={index}
                       label={label}
                       onChange={onNonFieldOrderChange}
                />

                <FieldType canEditFieldName={canEditFieldName}
                           fieldName={fieldName}
                           fieldType={fieldType}
                           index={index}
                           onChange={onNonFieldOrderChange}
                           recommendedDataTypes={recommendedDataTypes}
                />

                <FieldOrder canEditFieldName={canEditFieldName}
                            fieldName={fieldName}
                            fieldOrder={fieldOrder}
                            index={index}
                            onChange={onFieldOrderChange}
                            schemaLength={schemaLength}
                />

                <Categorical canEditFieldName={canEditFieldName}
                             categorical={categorical}
                             fieldName={fieldName}
                             fieldType={fieldType}
                             index={index}
                             onChange={onNonFieldOrderChange}
                />
            </div>

            {/*Hide on large screens*/}
            <div className={"d-flex d-lg-none flex-row"}>
                <FormatCode canEditFieldName={canEditFieldName}
                            fieldName={fieldName}
                            fieldType={fieldType}
                            formatCode={formatCode}
                            index={index}
                            onChange={onNonFieldOrderChange}/>
            </div>
        </div>
    )
};

SchemaRowInner.propTypes = {
    canEditFieldName: PropTypes.bool,
    categorical: PropTypes.bool,
    dispatchedUpdateSchema: PropTypes.func.isRequired,
    fieldName: PropTypes.string.isRequired,
    fieldOrder: PropTypes.number,
    fieldType: PropTypes.number.isRequired,
    formatCode: PropTypes.string,
    guessedVariableTypes: PropTypes.shape({
        types: PropTypes.shape({
            found: PropTypes.arrayOf(PropTypes.string),
            recommended: PropTypes.arrayOf(PropTypes.number)
        }),
        inFormats: PropTypes.shape({found: PropTypes.arrayOf(PropTypes.string)})
    }),
    index: PropTypes.number.isRequired,
    label: PropTypes.string,
};

export const SchemaRow = memo(SchemaRowInner);