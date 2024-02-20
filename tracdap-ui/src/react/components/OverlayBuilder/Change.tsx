/**
 * A component that shows an overlay menu so the variable to overlay can be set.
 *
 * @module Change
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {configuration} from "./config_overlay_builder";
import {convertSchemaToOptions} from "../../utils/utils_schema";
import {
    type OverlayBuilderStoreState,
    setApportionVariable,
    setDirection,
    setFieldTypeButton,
    setOverlayVariable,
    setValue,
    toggleApportion
} from "./overlayBuilderStore";
import {FieldTypeSelector} from "../FieldTypeSelector";
import {type FilterOptionOption} from "react-select/dist/declarations/src/filters";
import {hasOwnProperty, isDateFormat, isOption, isTracBasicType, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import {type Option} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "../SelectDate";
import {SelectOption} from "../SelectOption";
import {SelectToggle} from "../SelectToggle";
import {SelectValue} from "../SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Change component.
 */
export interface Props {

    /**
     * The index or position of the overlay in the array of overlays for the given overlayKey.
     */
    overlayIndex: number
    /**
     * A unique reference to the item having overlays applied to it. In a flow this can be the output node key as these are unique.
     */
    overlayKey: string
    /**
     *  The key in the OverlayBuilderStoreState to use to save the data to / get the data from.
     */
    storeKey: keyof OverlayBuilderStoreState["uses"]
}

export const Change = (props: Props) => {

    const {
        overlayIndex,
        overlayKey,
        storeKey,
    } = props

    // Get what we need from the store
    const {
        schema
    } = useAppSelector(state => state["overlayBuilderStore"].uses[storeKey].change[overlayKey].inputData)

    const {
        apportion,
        apportionVariable,
        direction,
        fieldTypes,
        overlayVariable,
        validationChecked,
        value
    } = useAppSelector(state => state["overlayBuilderStore"].uses[storeKey].change[overlayKey].changeTab[overlayIndex])

    // Does the overlay have a variable set
    const hasVariable = Boolean(overlayVariable != null)

    // The fieldType and formaCode for the selected variable, extracted from the chosen option
    const fieldType = overlayVariable?.details.schema.fieldType
    const formatCode = overlayVariable?.details.schema.formatCode

    // Get the format of any date or datetime variable and use that in the date value selector
    const dateFormat = hasVariable && isTracDateOrDatetime(fieldType) && formatCode ? formatCode.toLowerCase() : undefined

    // The schema converted to a set of options
    const allVariableOptions = useMemo(() => convertSchemaToOptions(schema || [], true,false, false), [schema])

    // The schema converted to a set of options, but for number variables only
    const allNumberVariableOptions = useMemo(() => convertSchemaToOptions((schema || []).filter(field => isTracNumber(field.fieldType)), true,false, false), [schema])

    /**
     * A function that is passed to the SelectOption component to use to filter the direction options based on what
     * field types the overlay variable is. The useCallback is an optimisation to prevent unnecessary re-renders. By
     * passing the function to do the filtering in the {@link SelectOption} component we avoid re-renders in the parent
     * component.
     *
     * @param option - The option in the select.
     * @param inputValue - The value of the search box in the select.
     */
    const directionSelectFilterOption = useCallback((option: FilterOptionOption<Option>, inputValue: string): boolean => {

        const {label, value} = option

        const optionBasicTypes = hasOwnProperty(option.data, "details") && hasOwnProperty(option.data.details, "basicTypes") && Array.isArray(option.data?.details?.basicTypes) ? option.data.details.basicTypes : []
        return Boolean((fieldType == null || optionBasicTypes.includes(fieldType)) && (inputValue === "" || label.toUpperCase().includes(inputValue.toUpperCase()) || value.toUpperCase().includes(inputValue.toUpperCase())))

    }, [fieldType])

    // Text to show below the value input to make it clear what the percentage value corresponds to
    const valueHelperText = direction?.value === "percent" && typeof value === "number" ? `This is ${value * 100}%` : undefined

    /**
     * A function that is passed to the SelectOption component to use to filter the overlay variable options based on what
     * field types the user has asked to show. The useCallback is an optimisation to prevent unnecessary re-renders. By
     * passing the function to do the filtering in the {@link SelectOption} component we avoid re-renders in the parent
     * component.
     *
     * @param option - The option in the select.
     * @param inputValue - The value of the search box in the select.
     */
    const overlayVariableSelectFilterOption = useCallback((option: FilterOptionOption<Option>, inputValue: string): boolean => {

        const {label, value} = option

        const optionFieldType = hasOwnProperty(option.data, "details") && hasOwnProperty(option.data.details, "schema") && hasOwnProperty(option.data.details.schema, "fieldType") && isTracBasicType(option.data?.details?.schema?.fieldType) && option.data?.details?.schema?.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET
        return Boolean(fieldTypes && fieldTypes.includes(optionFieldType) && (inputValue === "" || label.toUpperCase().includes(inputValue.toUpperCase()) || value.toUpperCase().includes(inputValue.toUpperCase())))

    }, [fieldTypes])

    return (

        <React.Fragment>

            <Row className={"pt-5"}>

                <Col xs={12} md={12} lg={6}>
                    <SelectOption basicType={trac.STRING}
                                  filterOption={overlayVariableSelectFilterOption}
                                  id={overlayIndex}
                                  isClearable={true}
                                  labelPosition={"left"}
                                  labelText={"Column to overlay:"}
                                  mustValidate={true}
                                  onChange={setOverlayVariable}
                                  options={allVariableOptions}
                                  showValidationMessage={true}
                                  validateOnMount={false}
                                  validationChecked={validationChecked}
                                  value={overlayVariable}
                    />

                </Col>
                <Col xs={12} md={12} lg={6}>
                    <div className={"d-flex justify-content-between flex-column h-100"}>
                        <div className={"d-grid d-md-flex"}>
                            <FieldTypeSelector buttonClassName={"fs-13 min-width-px-50"}
                                               dispatchedOnClick={setFieldTypeButton}
                                               fieldTypes={fieldTypes}
                                               id={overlayIndex}
                            />
                        </div>
                    </div>
                </Col>
            </Row>
            <Row className={"mt-2"}>

                <Col xs={6} md={3}>
                    <SelectOption basicType={trac.STRING}
                                  filterOption={directionSelectFilterOption}
                                  id={overlayIndex}
                                  mustValidate={true}
                                  onChange={setDirection}
                                  options={configuration.directionOptions}
                                  showValidationMessage={true}
                                  validationChecked={validationChecked}
                                  value={direction}
                    />

                </Col>

                <Col xs={6} md={apportion ? 2 : 3}>
                    {/*If no variable is set then set up the UI as if it was a string*/}
                    {((!hasVariable || isTracString(fieldType))) &&

                        <SelectValue basicType={trac.STRING}
                                     className={"flex-grow"}
                                     id={overlayIndex}
                                     mustValidate={true}
                                     onChange={setValue}
                                     showValidationMessage={true}
                                     validationChecked={validationChecked}
                                     validateOnMount={false}
                                     value={typeof value !== "string" ? null : value}
                        />
                    }

                    {/*If a null operator is set then these do not require a value to be set*/}
                    {isTracNumber(fieldType) &&

                        <SelectValue basicType={fieldType}
                                     id={overlayIndex}
                                     mustValidate={true}
                                     onChange={setValue}
                                     showValidationMessage={valueHelperText == null}
                                     validationChecked={validationChecked}
                                     validateOnMount={false}
                                     value={typeof value !== "number" ? null : value}
                                     helperText={valueHelperText}
                        />
                    }

                    {hasVariable && isTracBoolean(fieldType) &&
                        <SelectOption basicType={trac.BOOLEAN}
                                      id={overlayIndex}
                                      mustValidate={true}
                                      onChange={setValue}
                                      options={configuration.booleanOptions}
                                      showValidationMessage={true}
                                      validateOnMount={false}
                                      validationChecked={validationChecked}
                                      value={isOption(value) ? value : null}
                        />
                    }

                    {hasVariable && isTracDateOrDatetime(fieldType) &&
                        <SelectDate basicType={fieldType}
                                    formatCode={isDateFormat(dateFormat) ? dateFormat : null}
                                    id={overlayIndex}
                                    mustValidate={true}
                                    onChange={setValue}
                                    showValidationMessage={true}
                                    validationChecked={validationChecked}
                                    validateOnMount={false}
                                    value={typeof value !== "string" ? null : value}
                        />
                    }
                </Col>

                <Col xs={6} md={2}>

                    {/*You can not apportion an overlay if a single value is being set*/}
                    {isTracNumber(fieldType) && direction?.value !== "=" &&
                        <SelectToggle basicType={trac.BOOLEAN}
                                      id={overlayIndex}
                                      isDisabled={!isTracNumber(fieldType)}
                                      labelPosition={"left"}
                                      labelText={"Apportion:"}
                                      mustValidate={false}
                                      onChange={toggleApportion}
                                      showValidationMessage={true}
                                      validateOnMount={false}
                                      validationChecked={validationChecked}
                                      value={apportion}
                        />
                    }

                </Col>

                <Col xs={6} md={5}>

                    {isTracNumber(fieldType) && apportion &&
                        <SelectOption basicType={trac.STRING}
                                      id={overlayIndex}
                                      mustValidate={apportion}
                                      onChange={setApportionVariable}
                                      options={allNumberVariableOptions}
                                      showValidationMessage={true}
                                      placeHolderText={"Please select variable to apportion by"}
                                      validateOnMount={false}
                                      validationChecked={validationChecked}
                                      value={isOption(apportionVariable) ? apportionVariable : null}
                        />
                    }

                </Col>
            </Row>
        </React.Fragment>
    )
};

Change.propTypes = {

    overlayIndex: PropTypes.number.isRequired,
    overlayKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};