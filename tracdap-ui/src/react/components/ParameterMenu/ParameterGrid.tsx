/**
 * A component that shows a grid of parameter options under different categories or in tabs.
 *
 * @module ParameterGrid
 * @category Component
 */

import ErrorBoundary from "../ErrorBoundary";
import type {Option, SelectOptionProps} from "../../../types/types_general";
import {ParameterOption} from "./ParameterOption";
import PropTypes from "prop-types";
import React, {memo} from "react";
import Row from "react-bootstrap/Row";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";
import {SelectPayload} from "../../../types/types_general";

/**
 * An interface for the props of the ParameterGrid component.
 */
export interface Props {

    /**
     * A grid width for the xl and xxl Bootstrap sizes, on larger screens you may want to show three parameters
     * a row (baseWidth = 4) but dataset option two per row (baseWidth = 6) as there is more info to show in the
     * input. This prop controls this. On smaller screens you are more hampered and width is not set by prop.
     */
    baseWidth?: number
    /**
     * The css class to apply to the grid, this allows additional styles to be added to the component.
     * @defaultValue ''
     */
    className?: string
    /**
     * Whether to hide disabled options in the {@link SelectOption} component.
     */
    hideDisabledOptions?: boolean
    /**
     * Whether to select components in the menu are disabled.
     * @defaultValue false
     */
    isDisabled?: boolean
    /**
     * The key of the last parameter changed by the user. This is a rendering optimisation that is needed
     * because when the user changes a parameter the whole parameter object updates and this triggers a
     * full rerender of the parameter menu. To get around this we memoize the ParameterGrid component so
     * that it only re-renders if the updated parameter is inside it. Other grids don't update. If set to null
     * then the optimisation is not used. This is not used in the render phase.
     */
    lastParameterChanged: null | string
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
     * Whether to show the key for each option in the menu rather than the label. This is useful when trying to
     * load up datasets and get the keys of options that have not been set yet.
     */
    showKeysInsteadOfLabels?: boolean
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
     * A key that is attached to each parameter and returned with the onChange function and that references a key
     * in a store for what to update.
     */
    storeKey?: string
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

const ParameterGrid = (props: Props) => {

    console.log("Rendering ParameterGrid")

    const {
        baseWidth,
        className = "",
        isDisabled = false,
        hideDisabledOptions,
        onChange,
        parameterKeyList,
        parameters,
        policyValues,
        selectOptionObjectType,
        selectOptionOnCreateNewOption,
        showKeysInsteadOfLabels,
        storeKey,
        values,
        validationChecked
    } = props

    return (

        <ErrorBoundary>

            <Row className={className}>

                {parameterKeyList.map(key =>

                    <ParameterOption baseWidth={baseWidth}
                                     basicType={parameters[key].basicType}
                                     description={parameters[key].description}
                                     disabled={parameters[key].disabled || isDisabled}
                                     key={key}
                                     formatCode={parameters[key].formatCode}
                                     hidden={parameters[key].hidden}
                                     hideDisabledOptions={hideDisabledOptions}
                                     name={showKeysInsteadOfLabels ? key : parameters[key].name}
                                     isMulti={parameters[key].isMulti}
                                     maximumValue={parameters[key].maximumValue}
                                     minimumValue={parameters[key].minimumValue}
                                     mustValidate={parameters[key].mustValidate}
                                     onChange={onChange}
                                     options={parameters[key].options}
                                     parameterKey={key}
                                     selectOptionObjectType={selectOptionObjectType}
                                     selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                                     specialType={parameters[key].specialType}
                                     storeKey={storeKey}
                                     textareaRows={parameters[key].numberOfRows}
                                     tooltip={parameters[key].tooltip}
                                     validationChecked={validationChecked}
                                     value={values[key]}
                                     policyValue={policyValues?.[key]}
                                     widthMultiplier={parameters[key].widthMultiplier}
                    />
                )}
            </Row>
        </ErrorBoundary>
    )
};

ParameterGrid.propTypes = {

    baseWidth: PropTypes.number,
    className: PropTypes.string,
    isDisabled: PropTypes.bool,
    lastParameterChanged: PropTypes.string,
    onChange: PropTypes.func.isRequired,
    parameterKeyList: PropTypes.arrayOf(PropTypes.string),
    parameters: PropTypes.objectOf(
        PropTypes.shape({
            basicType: PropTypes.oneOf([trac.BOOLEAN, trac.DATE, trac.DATETIME, trac.DECIMAL, trac.FLOAT, trac.STRING, trac.INTEGER]),
            category: PropTypes.string,
            description: PropTypes.string,
            disabled: PropTypes.bool,
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
    values: PropTypes.objectOf(
        PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.bool, PropTypes.object, PropTypes.array])
    ),
    validationChecked: PropTypes.bool,
    selectOptionObjectType: PropTypes.number,
    selectOptionOnCreateNewOption: PropTypes.func
};

// This is a re-rendering optimisation to only update a ParameterGrid when the option changed is inside it
//export default memo(ParameterGrid, (prevProps, nextProps) => Boolean(prevProps["validationChecked"] === nextProps["validationChecked"] && (nextProps["lastParameterChanged"] != null && !prevProps["parameterKeyList"].includes(nextProps["lastParameterChanged"]))));
export default memo(ParameterGrid)