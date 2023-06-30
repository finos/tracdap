/**
 * A component that places the parameter grid in a list rather than in a tab.
 *
 * @module ParameterGridInList
 * @category Component
 */

import {convertKeyToText} from "../../utils/utils_string";
import {HeaderTitle} from "../HeaderTitle";
import type {Option, SelectOptionProps, SelectPayload} from "../../../types/types_general";
import ParameterGrid from "./ParameterGrid";
import React from "react";
import PropTypes from "prop-types";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the ParameterGridInList component.
 */
export interface Props {

    /**
     * A grid width for the xl and xxl Bootstrap sizes, on larger screens you may want to show three parameters
     * a row (baseWidth = 4) but dataset option two per row (baseWidth = 6) as there is more info to show in the input.
     * This prop controls this. On smaller screens you are more hampered and width is not set by prop.
     */
    baseWidth?: number
    /**
     * The category of attributes/parameters in the grid.
     */
    category: string
    /**
     * Whether to hide disabled options in the {@link SelectOption} component.
     */
    hideDisabledOptions?: boolean
    /**
     * Whether to select components in the menu are disabled.
     */
    isDisabled?: boolean
    /**
     * The text to show along with the category name e.g. GeneralChartConfig attributes
     */
    label?: "attributes" | "parameters" | "input datasets" | ""
    /**
     * The key of the last parameter changed by the user. This is a rendering optimisation that is needed
     * because when the user changes a parameter the whole parameter object updates and this triggers a
     * full rerender of the parameter menu. To get around this we memoize the ParameterGrid component so
     * that it only re-renders if the updated parameter is inside it. Other grids don't update. If set to null
     * then the optimisation is not used. This is not used in the render phase.
     */
    lastParameterChanged: null | string
    /**
     * A key that is attached to each parameter and returned with the onChange function and that references a key
     * in a store for what to update.
     */
    storeKey?: string
    /**
     * Function to run when an attribute/parameter is changed.
     */
    onChange: Function
    /**
     * The list of attributes/parameters in the given category.
     */
    parameterKeyList: string[]
    /**
     * The processed attributes/parameters object. This is the dataset from TRAC processed to have all the options,
     * default and minimum and maximum values set. This is created by the processAttributes util function.
     */
    parameters: Record<string, UiAttributesProps>
    /**
     * The values of the attributes/parameters that are considered in policy, when the user selects values equal to
     * these we show additional feedback that the values match these. This is used for example
     * when re-running a job.
     */
    policyValues?: Record<string, SelectPayload<Option, boolean>["value"]>
    /**
     * When there are parameters using the {@link SelectOption} component then if the user is allowed to
     * create options we can specify what TRAC object type the created option can be if the user is
     * entering an object ID or object key.
     */
    selectOptionObjectType?: SelectOptionProps["objectType"]
    /**
     * When there are parameters using the {@link SelectOption} component then if the user is allowed to
     * create options we can specify what function to run to add the new option to the store.
     */
    selectOptionOnCreateNewOption?: SelectOptionProps["onCreateNewOption"]
    /**
     * Whether to show the header title or not. If all the attributes/parameters are in a single category then the
     * title can be hidden as it really only makes sense to show them when there are multiple categories.
     */
    showHeader: boolean
    /**
     * Whether to show the key for each option in the menu rather than the label. This is useful when trying to
     * load up datasets and get the keys of options that have not been set yet.
     * @defaultValue false
     */
    showKeysInsteadOfLabels?: boolean
    /**
     * An object keyed by the parameter key containing the selected values for the attributes/parameters.
     */
    values: Record<string, SelectPayload<Option, boolean>["value"]>
    /**
     * Whether the validation of the attribute/parameter values has been performed, for example if the user has tried
     * to kick of a model. When true the validation messages will show if there are any issues found.
     */
    validationChecked: boolean
}

export const ParameterGridInList = (props: Props) => {

    const {
        baseWidth,
        category,
        hideDisabledOptions,
        isDisabled,
        label,
        lastParameterChanged,
        onChange,
        parameterKeyList,
        parameters,
        policyValues,
        selectOptionObjectType,
        selectOptionOnCreateNewOption,
        showHeader,
        showKeysInsteadOfLabels = false,
        storeKey,
        validationChecked,
        values
    } = props

    return (
        <React.Fragment>

            {showHeader && parameterKeyList.some(key => !parameters[key].hidden) &&
                <HeaderTitle outerClassName={"my-4"}
                             text={`${convertKeyToText(category)} ${label || ""}`}
                             type={"h4"}
                />
            }

            <ParameterGrid baseWidth={baseWidth}
                           hideDisabledOptions={hideDisabledOptions}
                           isDisabled={isDisabled}
                           storeKey={storeKey}
                           parameters={parameters}
                           values={values}
                           policyValues={policyValues}
                           parameterKeyList={parameterKeyList}
                           onChange={onChange}
                           lastParameterChanged={lastParameterChanged}
                           selectOptionObjectType={selectOptionObjectType}
                           selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                           showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                           validationChecked={validationChecked}
            />
        </React.Fragment>
    )
}

ParameterGridInList.propTypes = {

    baseWidth: PropTypes.number,
    category: PropTypes.string,
    lastParameterChanged: PropTypes.string,
    isDisabled: PropTypes.bool,
    onChange: PropTypes.func.isRequired,
    parameterKeyList: PropTypes.arrayOf(PropTypes.string),
    parameters: PropTypes.objectOf(
        PropTypes.shape({
            basicType: PropTypes.oneOf([trac.BOOLEAN, trac.DATE, trac.DATETIME, trac.DECIMAL, trac.FLOAT, trac.STRING, trac.INTEGER]),
            category: PropTypes.string,
            description: PropTypes.string,
            name: PropTypes.string,
            formatCode: PropTypes.string,
            hidden: PropTypes.bool.isRequired,
            isMulti: PropTypes.bool.isRequired,
            maximumValue: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
            minimumValue: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
            mustValidate: PropTypes.bool.isRequired,
            options: PropTypes.oneOfType([

                PropTypes.arrayOf(PropTypes.shape({
                    value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]),
                    label: PropTypes.string.isRequired,
                    disabled: PropTypes.bool,
                    type: PropTypes.object,
                    details: PropTypes.object,
                    tag: PropTypes.object,
                    tagHeader: PropTypes.object
                })).isRequired,
                PropTypes.arrayOf(
                    PropTypes.shape({
                            label: PropTypes.string,
                            options: PropTypes.arrayOf(
                                PropTypes.shape({
                                    value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]).isRequired,
                                    label: PropTypes.string.isRequired,
                                    disabled: PropTypes.bool,
                                    type: PropTypes.object,
                                    details: PropTypes.object,
                                    tag: PropTypes.object,
                                    tagHeader: PropTypes.object

                                }).isRequired
                            )
                        }
                    )
                ).isRequired
            ]),
            id: PropTypes.string.isRequired,
            specialType: PropTypes.oneOf(["OPTION", "STRING", "TEXTAREA"]),
            numberOfRows: PropTypes.number,
            tooltip: PropTypes.string,
            widthMultiplier: PropTypes.number.isRequired
        })),
    showHeader: PropTypes.bool,
    values: PropTypes.objectOf(
        PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.bool, PropTypes.object, PropTypes.array])
    ),
    validationChecked: PropTypes.bool,
    selectOptionObjectType: PropTypes.number,
    selectOptionOnCreateNewOption: PropTypes.func
};