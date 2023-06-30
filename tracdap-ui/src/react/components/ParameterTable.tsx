/**
 * A component that shows information about parameters from a model's metadata definition. This component is for
 * viewing in a browser, there is a sister component called {@link ParameterTablePdf} that is for viewing in a
 * PDF. These two components need to be kept in sync so if a change is made to one then it should be reflected in
 * the other.
 *
 * @module ParameterTable
 * @category Component
 */

import {Badges} from "./Badges";
import {convertBasicTypeToString, extractValueFromTracValueObject} from "../utils/utils_trac_metadata";
import {hasOwnProperty, isPrimitive, isValue} from "../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {Option} from "../../types/types_general";
import {areSelectValuesEqual} from "../utils/utils_attributes_and_parameters";
import {PolicyStatusIcon} from "./PolicyStatusIcon";

/**
 * An interface for the props of the ParameterTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The params object of a TRAC model. This is keyed by the parameter key. Model parameters are of type
     * IModelParameter which has a defaultValue property of type IValue. Jobs and flows can have parameters with the
     * IValue type.
     */
    params?: null | Record<string, trac.metadata.IModelParameter | (trac.metadata.IValue & { label?: string })>
    /**
     * The values of the attributes/parameters that are considered in policy, when the user selects values equal to
     * these we show additional feedback that the values match these. This is used for example
     * when re-running a job.
     */
    policyValues?: Record<string, null | string | boolean | number | Option | Option[] | readonly Option[]>
}

export const ParameterTable = (props: Props) => {

    const {className = "", params, policyValues} = props

    // The header for the value column, TRAC job values and model parameters can both be shown in the table but model
    // parameters have a default value rather than an actual value, so we have to differentiate.
    const valueColumnLabel = params && Object.keys(params).length > 0 && isValue(params[Object.keys(params)[0]]) ? "Value" : "Default value"

    return (

        <React.Fragment>

            {(!params || Object.keys(params).length === 0) &&
                <div className={"pb-1"}>There are no parameters</div>
            }

            {params && Object.keys(params).length > 0 &&
                <Table responsive className={`dataHtmlTable ${className}`}>
                    <thead>
                    <tr>
                        <th>Key</th>
                        <th>Name</th>
                        <th className={"text-center"}>Type</th>
                        <th className={"text-center"}>{valueColumnLabel}</th>
                        {
                            policyValues && <th className={"text-center"}>Policy compliance</th>
                        }
                    </tr>
                    </thead>
                    <tbody>
                    {Object.entries(params).map(([key, paramObject]) => {

                        const values = !isValue(paramObject) ? extractValueFromTracValueObject(paramObject.defaultValue) : extractValueFromTracValueObject(paramObject)
                        const basicType = !isValue(paramObject) ? (paramObject?.paramType?.basicType ? convertBasicTypeToString(paramObject.paramType?.basicType, true) : "Not set") : (paramObject?.type?.basicType ? convertBasicTypeToString(paramObject.type?.basicType, true) : "Not set")

                        // If a policy value is set then work out if the selected value matches the required policy value
                        const isValueAlignedWithPolicy = hasOwnProperty(policyValues, key) && isPrimitive(values.value) ? areSelectValuesEqual(policyValues[key], values.value) : undefined

                        return (
                            <tr key={key}>
                                <td className={"user-select-all"}>{key}</td>
                                <td>{paramObject.label || "Not set"}</td>
                                <td className={"text-center"}>{basicType}</td>
                                <td className={"text-center"}>
                                    {/*This handles arrays and objects*/}
                                    {values.value === null ? "Not set" :
                                        isPrimitive(values.value) ?
                                            <span className={"user-select-all"}>{values.value.toString()}</span> :
                                            <Badges convertText={true}
                                                    text={values.value}
                                            />
                                    }
                                </td>
                                {policyValues &&
                                    <td className={"text-center"}>
                                        <PolicyStatusIcon coveredInPolicy={hasOwnProperty(policyValues, key)}
                                                          isValueAlignedWithPolicy={isValueAlignedWithPolicy}
                                        />
                                    </td>
                                }
                            </tr>
                        )
                    })}
                    </tbody>
                </Table>
            }

        </React.Fragment>
    )
}

ParameterTable.propTypes = {

    className: PropTypes.string,
    params: PropTypes.object,
    policyValues: PropTypes.object
};