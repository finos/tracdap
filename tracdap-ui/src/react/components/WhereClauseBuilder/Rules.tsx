/**
 * A component that shows the rules that make up the where clause. This needs to be in a separate
 * component to WhereClauseBuilder due to the render logic to deal with when the store has not got
 * the object ID already stored for the dataset.
 *
 * @module Rules
 * @category Component
 */

import {convertSchemaToOptions} from "../../utils/utils_schema";
import ErrorBoundary from "../ErrorBoundary";
import {Logic} from "./Logic";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {Rule} from "./Rule";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";
import {type WhereClauseBuilderStoreState} from "./whereClauseBuilderStore";

/**
 * An interface for the props of the Rules component.
 */
export interface Props {

    /**
     * The object key of the data the user has selected to query, the key include the object version. The
     * store has a separate area for each dataset that the user has loaded up into QueryBuilder.
     */
    objectKey: string
    /**
     * The TRAC schema for the dataset.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof WhereClauseBuilderStoreState["uses"]
    /**
     * The index of the where clause, individual objectKey value can have multiple where clauses. For
     * example in OverlayBuilder the objectKey is the dataset key, a dataset can have multiple overlays
     * defined, each with a where clause with multiple rules.
     */
    whereIndex: number
}

export const Rules = (props: Props) => {

    const {objectKey, schema, storeKey, whereIndex} = props

    // Get what we need from the store
    const {rules} = useAppSelector(state => state["whereClauseBuilderStore"].uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion)
    const {validationChecked} = useAppSelector(state => state["whereClauseBuilderStore"].uses[storeKey].whereClause[objectKey])

    // The schema converted to a set of options
    const variableOptions = useMemo(() => convertSchemaToOptions(schema, true,false,  false), [schema])

    return (

        <ErrorBoundary>
            {rules.map((rule, i) =>

                <React.Fragment key={i}>
                    <Rule ruleIndex={i}
                          rule={rule}
                          variableOptions={variableOptions}
                          validationChecked={validationChecked}
                          whereIndex={whereIndex}
                    />

                    {i !== rules.length - 1 &&
                    <Logic ruleIndex={i}
                           logic={rule.logic}
                           not={rule.not}
                           whereIndex={whereIndex}
                    />
                    }
                </React.Fragment>
            )}
        </ErrorBoundary>
    )
};

Rules.propTypes = {

    objectKey: PropTypes.string.isRequired,
    schema: PropTypes.array.isRequired,
    storeKey: PropTypes.string.isRequired,
    whereIndex: PropTypes.number.isRequired
};