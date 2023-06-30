/**
 * A component that shows a dataset's field schema and allows them to edit any field's schema properties. It uses two
 * subcomponents to render the titles and each field in the schema.
 *
 * @module SchemaEditor
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import type {GuessedVariableTypes, UpdateSchemaPayload} from "../../../types/types_general";
import PropTypes from "prop-types";
import React from "react";
import {SchemaHeader} from "./SchemaHeader";
import {SchemaRow} from "./SchemaRow";
import {tracdap as trac} from "@finos/tracdap-web-api";

// The value for the recommended types when the fieldName is not set, this should not happen but the TRAC interfaces include
// null and undefined values for fieldName. This is set outside the component as a rendering optimisation.
const fallBackRecommendedType: trac.BasicType[] = [trac.STRING]

/**
 * An interface for the props of the SchemaEditor component.
 */
export interface Props {

    /**
     * Whether the fieldName can be edited. When creating a schema you want to be able to edit it but not when
     * editing the schema of a dataset loaded by CSV or Excel.
     */
    canEditFieldName: boolean
    /**
     * The function to run to update the edited schema in the store.
     */
    dispatchedUpdateSchema: ActionCreatorWithPayload<UpdateSchemaPayload>
    /**
     * The types and formats that the guessVariableTypes util function says will work with the data.
     */
    guessedVariableTypes: GuessedVariableTypes
    /**
     * The schema to edit.
     */
    schema: trac.metadata.IFieldSchema[]
}

export const SchemaEditor = (props: Props) => {

    console.log("Rendering SchemaEditor")

    const {
        canEditFieldName,
        dispatchedUpdateSchema,
        guessedVariableTypes,
        schema,
    } = props;

    return (

        <React.Fragment>
            {!canEditFieldName && <SchemaHeader/>}
            <div className={"schema-editor"}>
                {schema.map((field, i) => (
                    <SchemaRow categorical={field.categorical}
                               canEditFieldName={canEditFieldName}
                               dispatchedUpdateSchema={dispatchedUpdateSchema}
                               fieldName={field.fieldName}
                               fieldOrder={field.fieldOrder}
                               fieldType={field.fieldType}
                               formatCode={field.formatCode}
                               index={i}
                               key={field.fieldName}
                               label={field.label}
                               recommendedDataTypes={field.fieldName ? guessedVariableTypes[field.fieldName].types.recommended : fallBackRecommendedType}
                               schemaLength={schema.length}
                    />)
                )}
            </div>
        </React.Fragment>
    )
};

SchemaEditor.propTypes = {

    dispatchedUpdateSchema: PropTypes.func.isRequired,
    guessedVariableTypes: PropTypes.objectOf(
        PropTypes.shape({
            types: PropTypes.shape({
                found: PropTypes.arrayOf(PropTypes.string),
                recommended: PropTypes.arrayOf(PropTypes.number)
            }),
            inFormats: PropTypes.shape({found: PropTypes.arrayOf(PropTypes.string)})
        })),
    schema: PropTypes.array.isRequired,
};