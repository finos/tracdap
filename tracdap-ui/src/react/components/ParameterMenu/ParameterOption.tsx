/**
 * A component that shows a title, description and widget for a metadata attribute or model parameter.
 *
 * @module ParameterOption
 * @category Component
 */

import {areSelectValuesEqual} from "../../utils/utils_attributes_and_parameters";
import Col from "react-bootstrap/Col";
import {convertKeyToText} from "../../utils/utils_string";
import {HeaderTitle} from "../HeaderTitle";
import {Icon} from "../Icon";
import {isDateFormat, isMultiOption, isOption, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import type {Option, SelectOptionProps, SelectPayload} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {memo} from "react";
import {SelectDate} from "../SelectDate";
import {SelectOption} from "../SelectOption";
import {SelectToggle} from "../SelectToggle";
import {SelectValue} from "../SelectValue";
import {TextBlock} from "../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the ParameterOption component.
 */
export interface Props {

    /**
     * A grid width for the xl and xxl Bootstrap sizes, on larger screens you may want to show three parameters
     * a row (baseWidth = 4) but dataset option two per row (baseWidth = 6) as there is more info to show in the input.
     * This prop controls this. On smaller screens you are more hampered and width is not set by prop.
     * @defaultValue 4
     */
    baseWidth?: number
    /**
     * The TRAC basicType for the attribute/parameter. This is sn enum mapping to the TRAC types e.g. 1 = BOOLEAN, 2 = INTEGER.
     */
    basicType: UiAttributesProps["basicType"]
    /**
     * The css class to apply to the widget, this allows additional styles to be added to the component.
     * @defaultValue ''
     */
    className?: string
    /**
     * The description of the attribute/parameter.
     */
    description: UiAttributesProps["description"]
    /**
     * Whether the attribute/parameter can be changed.
     */
    disabled: UiAttributesProps["disabled"]
    /**
     * The format to apply to the attribute/parameter value, this is for date/datetime and number attributes/parameters.
     */
    formatCode: UiAttributesProps["formatCode"]
    /**
     * Whether the attribute/parameter option widget should be shown. If hidden then the attribute/parameter will still
     * be sent with the others and will use the default value. This is useful if you want to pass attributes/parameters
     * that the user does not need to interact with.
     * @defaultValue false
     */
    hidden: UiAttributesProps["hidden"]
    /**
     * Whether to hide disabled options in the {@link SelectOption} component.
     */
    hideDisabledOptions?: boolean
    /**
     * Whether the action needs to be dispatched or not. Thunks need to be dispatched. This component is part of the broader
     * {@link ParameterMenu} component which does not pass down this prop, so all actions are dispatched (the default of
     * the select components is to dispatch) but we have this prop in case you just want to use this component on its own,
     * this is done in the {@link ViewAttributeModal}.
     */
    isDispatched?: boolean
    /**
     * Whether the user can select multiple values from a set of options or create multiple values.
     * @defaultValue false
     */
    isMulti: UiAttributesProps["isMulti"]
    /**
     * The maximum allowed value or length for the attribute/parameter. If mustValidate is true and the attribute/parameter
     * is above this allowed then validation errors will show.
     */
    maximumValue: UiAttributesProps["maximumValue"]
    /**
     * The minimum allowed value or length for the attribute/parameter. If mustValidate is true and the attribute/parameter
     * is below this minimum then validation errors will show.
     */
    minimumValue: UiAttributesProps["minimumValue"]
    /**
     * Whether the attribute/parameter must be validated before use in a subsequent process e.g. saving the values in TRAC.
     */
    mustValidate: UiAttributesProps["mustValidate"]
    /**
     * The title or name of the attribute/parameter.
     */
    name: UiAttributesProps["name"]
    /**
     * Function to run when an attribute/parameter is changed.
     */
    onChange: Function
    /**
     * An array of options to use if the attribute/parameter widget is for a SelectOption component.
     */
    options: UiAttributesProps["options"]
    /**
     * The key of the attribute/parameter this is needed to be able to know which parameter is being changed. This is
     * equal to the parameter ID.
     */
    parameterKey: UiAttributesProps["id"]
    /**
     * The value of the attribute/parameter that is considered in policy, when the user selects values equal to
     * these we show additional feedback that the value matches this. This is used for example
     * when re-running a job.
     */
    policyValue?: SelectPayload<Option, boolean>["value"]
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
     * The attribute/parameter may need some special treatment to put it in the right widget, for example to be a password
     * widget or allow the user to pick options. This prop has the information what if any special treatment is needed.
     */
    specialType: UiAttributesProps["specialType"]
    /**
     * A key that is attached to each parameter and returned with the onChange function and that references a key
     * in a store for what to update.
     */
    storeKey?: string
    /**
     * If greater than one then string attributes/parameters will be shown in a text area (rather than an input) with
     * this number of rows.
     */
    textareaRows?: UiAttributesProps["textareaRows"]
    /**
     * The tooltip text to show when the attribute/parameter title icon is hovered over.
     */
    tooltip: UiAttributesProps["tooltip"]
    /**
     * Whether the validation of the attribute/parameter values has been performed, for example if the user has tried
     * to kick of a model. When true the validation messages will show if there are any issues found.
     */
    validationChecked: boolean
    /**
     * The value for the attribute/parameter.
     */
    value: SelectPayload<Option, boolean>["value"]
    /**
     * A multiplier to make the width of the attribute/parameter widget larger by a user defined factor. This is useful
     * for text areas for example which tend to need to be wider. For example on a large screen a widget is usually four
     * grid columns wide, a widthMultiplier of three would make this 12 columns wide (or full width).
     * @defaultValue 1
     */
    widthMultiplier: UiAttributesProps["widthMultiplier"]
}

const ParameterOptionInner = (props: Props) => {

    const {
        baseWidth = 4,
        basicType,
        className = "",
        description,
        disabled,
        formatCode,
        hidden = false,
        hideDisabledOptions,
        isDispatched,
        isMulti = false,
        maximumValue,
        minimumValue,
        mustValidate,
        name,
        storeKey,
        onChange,
        options,
        parameterKey,
        policyValue,
        selectOptionObjectType,
        selectOptionOnCreateNewOption,
        specialType,
        textareaRows,
        tooltip,
        validationChecked,
        value,
        widthMultiplier = 1
    } = props

    console.log(props)

    const finalClassName = `mb-2 ${className} ${hidden ? "d-none" : ""}`
    const finalWidthMultiplier = widthMultiplier ? widthMultiplier : 1

    // If a policy value is set then work out if the selected value matches the required policy value
    const isValueAlignedWithPolicy = policyValue !== undefined ? areSelectValuesEqual(policyValue, value) : undefined

    return (

        <Col xs={12}
             md={12}
             lg={Math.min(12, 6 * finalWidthMultiplier)}
             xl={Math.min(12, baseWidth * finalWidthMultiplier)}
             xxl={Math.min(12, baseWidth * finalWidthMultiplier)}
             className={finalClassName}
        >

            <div className={"d-flex align-items-start flex-column h-100"}>
                <HeaderTitle outerClassName={"mt-0 mb-3 w-100"}
                             text={name || convertKeyToText(parameterKey)}
                             tooltip={tooltip}
                             type={"h5"}

                >
                    {policyValue === undefined ? undefined : <Icon ariaLabel={isValueAlignedWithPolicy ? "In policy" : "Not in policy"}
                                                                   className={isValueAlignedWithPolicy ? "mx-1 text-success" : "mx-1 text-danger"}
                                                                   colour={null}
                                                                   icon={isValueAlignedWithPolicy ? "bi-check-circle" : "bi-x-circle"}
                                                                   tooltip={isValueAlignedWithPolicy ? "In policy" : "Not in policy"}
                    />}

                </HeaderTitle>

                {description &&
                    <TextBlock className={`mb-3 fs-8`}>
                        {description}
                    </TextBlock>
                }

                <div className={"w-100 mt-auto"}>
                    {specialType === undefined && basicType === trac.BOOLEAN &&
                        <SelectToggle onChange={onChange}
                                      id={parameterKey}
                                      isDisabled={disabled}
                                      isDispatched={isDispatched}
                                      mustValidate={mustValidate}
                                      validationChecked={validationChecked}
                                      showValidationMessage={true}
                                      storeKey={storeKey}
                                      value={typeof value === "boolean" ? value : null}
                        />
                    }

                    {specialType !== "OPTION" && !isMulti && isTracNumber(basicType) &&
                        <SelectValue basicType={basicType}
                                     onChange={onChange}
                                     value={typeof value === "number" ? value : null}
                                     id={parameterKey}
                                     isDisabled={disabled}
                                     isDispatched={isDispatched}
                                     rows={textareaRows}
                                     mustValidate={mustValidate}
                                     validationChecked={validationChecked}
                                     minimumValue={typeof minimumValue === "number" ? minimumValue : null}
                                     maximumValue={typeof maximumValue === "number" ? maximumValue : null}
                                     showValidationMessage={true}
                                     storeKey={storeKey}
                        />
                    }

                    {specialType !== "OPTION" && !isMulti && isTracString(basicType) &&
                        // @ts-ignore
                        <SelectValue basicType={basicType}
                                     onChange={onChange}
                                     value={typeof value === "string" ? value : null}
                                     id={parameterKey}
                                     isDisabled={disabled}
                                     isDispatched={isDispatched}
                                     rows={props.textareaRows}
                                     mustValidate={mustValidate}
                                     validationChecked={validationChecked}
                                     minimumValue={typeof minimumValue === "number" ? minimumValue : null}
                                     maximumValue={typeof maximumValue === "number" ? maximumValue : null}
                                     showValidationMessage={true}
                                     specialType={specialType}
                                     storeKey={storeKey}
                        />
                    }

                    {specialType !== "OPTION" && !isMulti && isTracDateOrDatetime(basicType) &&
                        <SelectDate basicType={basicType}
                                    className={"w-100"}
                                    formatCode={isDateFormat(formatCode) ? formatCode : null}
                                    id={parameterKey}
                                    isClearable={true}
                                    isDisabled={disabled}
                                    isDispatched={isDispatched}
                                    maximumValue={typeof maximumValue === "string" ? maximumValue : null}
                                    minimumValue={typeof minimumValue === "string" ? minimumValue : null}
                                    mustValidate={mustValidate}
                                    onChange={onChange}
                                    showValidationMessage={true}
                                    storeKey={storeKey}
                                    validationChecked={validationChecked}
                                    value={typeof value === "string" ? value : null}
                        />
                    }

                    {basicType && (specialType === "OPTION" || isMulti) &&
                        // If a parameter is multivalued but does not have options set then allow it to be creatable
                        // but with no options. Note that if objectType and onCreateNewOption are set then we are
                        // allowing new items to be added but these will be for TRAC objects.
                        <SelectOption basicType={basicType}
                                      hideDisabledOptions={hideDisabledOptions}
                                      id={parameterKey}
                                      isClearable={true}
                                      isCreatable={Boolean((specialType !== "OPTION" && isMulti) || (selectOptionObjectType !== undefined && selectOptionOnCreateNewOption))}
                                      isDisabled={disabled}
                                      isDispatched={isDispatched}
                                      isMulti={isMulti}
                                      objectType={selectOptionObjectType}
                                      onCreateNewOption={selectOptionOnCreateNewOption}
                                      onChange={onChange}
                                      options={specialType !== "OPTION" && isMulti ? undefined : options}
                                      mustValidate={mustValidate}
                                      showValidationMessage={true}
                                      storeKey={storeKey}
                                      validationChecked={validationChecked}
                                      value={isOption(value) || isMultiOption(value) || value === null ? value : null}
                        />
                    }
                </div>
            </div>
        </Col>
    )
};

ParameterOptionInner.propTypes = {

    baseWidth: PropTypes.number,
    basicType: PropTypes.oneOf([trac.BOOLEAN, trac.DATE, trac.DATETIME, trac.DECIMAL, trac.FLOAT, trac.STRING, trac.INTEGER]),
    className: PropTypes.string,
    description: PropTypes.string,
    name: PropTypes.string,
    formatCode: PropTypes.string,
    hidden: PropTypes.bool.isRequired,
    isMulti: PropTypes.bool.isRequired,
    maximumValue: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    minimumValue: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    mustValidate: PropTypes.bool,
    onChange: PropTypes.func.isRequired,
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
    parameterKey: PropTypes.string.isRequired,
    specialType: PropTypes.oneOf(["OPTION", "STRING", "TEXTAREA"]),
    textareaRows: PropTypes.number,
    tooltip: PropTypes.string,
    widthMultiplier: PropTypes.number.isRequired,
    validationChecked: PropTypes.bool.isRequired,
    values: PropTypes.shape({
        basicType: PropTypes.number.isRequired,
        id: PropTypes.string,
        name: PropTypes.string.isRequired,
        isValid: PropTypes.bool.isRequired,
        value: PropTypes.oneOfType([PropTypes.string, PropTypes.string, PropTypes.bool, PropTypes.object, PropTypes.array])
    }),
    selectOptionObjectType: PropTypes.number,
    selectOptionOnCreateNewOption: PropTypes.func
};

export const ParameterOption = memo(ParameterOptionInner);