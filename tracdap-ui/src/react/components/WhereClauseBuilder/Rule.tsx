/**
 * A component that shows a single rule menu for the rules that make up the where clause.
 *
 * @module Rule
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {configuration} from "./config_where_clause_builder";
import {ConfirmButton} from "../ConfirmButton";
import {deleteRule, type Rule as RuleType, setOperator, setValue1, setValue2, setWhereClauseVariable} from "./whereClauseBuilderStore";
import type {GenericGroup, GenericOption} from "../../../types/types_general";
import {HeaderTitle} from "../HeaderTitle";
import {Icon} from "../Icon";
import {isDateFormat, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {memo, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "../SelectDate";
import {SelectOption} from "../SelectOption";
import {SelectValue} from "../SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Rule component.
 */
export interface Props {

    /**
     * The rule to show in the user interface. The object contains all the details of the rule.
     */
    rule: RuleType
    /**
     * The index or position of the rule in the array of rules.
     */
    ruleIndex: number
    /**
     * Whether the rule has been validated and can be used.
     */
    validationChecked: boolean
    /**
     * The options for the variables in the schema.
     */
    variableOptions: GenericOption[] | GenericGroup[]
    /**
     * The index of the where clause, individual objectKey value can have multiple where clauses. For
     * example in OverlayBuilder the objectKey is the dataset key, a dataset can have multiple overlays
     * defined, each with a where clause with multiple rules.
     */
    whereIndex: number
}

const RuleInner = (props: Props) => {

    const {
        rule: {variable, operator, value1, value2},
        ruleIndex,
        validationChecked,
        variableOptions,
        whereIndex
    } = props

    // Does the rule have a variable set
    const hasVariable = Boolean(variable != null)

    // This is not value setter if the operator is set to identify null values
    const isNullOperator = ["is NULL", "is NOT NULL"].includes(operator.value)

    const isInOperator = ["in", "not in"].includes(operator.value)

    const isBetween = Boolean(operator.value === "between")

    // The fieldType and formaCode for the selected variable, extracted from the chosen option
    const fieldType = variable?.details.schema.fieldType
    const formatCode = variable?.details.schema.formatCode

    // Get the list of options we need
    const {booleanOptions, operatorOptions} = configuration

    // Get the operators that match the selected variable type, if there is no variable set get the ones for a float
    const filteredOperatorOptions = useMemo(() => operatorOptions.filter(operator => operator.details.basicTypes.includes(fieldType || trac.BasicType.FLOAT)), [operatorOptions, fieldType])

    // Get the format of any date or datetime variable and use that in the date value selector
    const dateFormat = hasVariable && isTracDateOrDatetime(fieldType) && formatCode ? formatCode.toLowerCase() : undefined

    return (

        <React.Fragment>

            <HeaderTitle type={"h5"} text={`Rule #${ruleIndex + 1}`} outerClassName={ruleIndex === 0 ? "mt-4 mb-3" : "mt-0 mb-3"}>
                <div className={"d-block d-lg-none"}>
                    <ConfirmButton ariaLabel={"Delete rule"}
                                   cancelText={"No"}
                                   className={"m-0 p-0"}
                                   confirmText={"Yes"}
                                   description={"Are you sure that you want to delete this rule?"}
                                   dispatchedOnClick={deleteRule}
                                   id={ruleIndex}
                                   index={whereIndex}
                                   variant={"link"}
                    >
                        <Icon ariaLabel={false}
                              icon={'bi-trash3'}
                        />
                    </ConfirmButton>
                </div>
            </HeaderTitle>
            <Row className={"pb-3"}>
                <Col xs={12} md={8} lg={5} className={"pb-2 pb-lg-0"}>
                    <SelectOption basicType={trac.STRING}
                                  id={ruleIndex}
                                  index={whereIndex}
                                  mustValidate={true}
                                  options={variableOptions}
                                  onChange={setWhereClauseVariable}
                                  showValidationMessage={false}
                                  validationChecked={validationChecked}
                                  validateOnMount={false}
                                  value={variable}
                    />
                </Col>

                {/*The 3 cols allows the delete icon to go on one line when using a null operator*/}
                <Col xs={12} md={isNullOperator ? 3 : 4} lg={2} className={"pb-2 pb-lg-0"}>
                    <SelectOption basicType={trac.STRING}
                                  id={ruleIndex}
                                  index={whereIndex}
                                  mustValidate={true}
                                  options={filteredOperatorOptions}
                                  onChange={setOperator}
                                  showValidationMessage={false}
                                  validationChecked={validationChecked}
                                  validateOnMount={false}
                                  value={operator}
                    />
                </Col>

                {/*The 5 cols allows the delete icon to go on one line when using a null operator*/}
                <Col xs={12} md={12} lg={5} className={`${isNullOperator ? "d-none" : "d-flex"} pb-2 pb-lg-0`}>

                    {/*If no variable is set then set up the UI as if it was a string*/}
                    {/*If a null operator is set then these do not require a value to be set*/}
                    {((!hasVariable || isInOperator || isTracString(fieldType)) && !isNullOperator) &&

                        <SelectValue basicType={trac.STRING}
                                     className={"flex-grow"}
                                     id={ruleIndex}
                                     index={whereIndex}
                                     mustValidate={true}
                                     onChange={setValue1}
                                     showValidationMessage={false}
                                     validationChecked={validationChecked}
                                     validateOnMount={false}
                                     value={typeof value1 !== "string" ? null : value1}
                        />
                    }

                    {/*If a null operator is set then these do not require a value to be set*/}
                    {isTracNumber(fieldType) && !isNullOperator &&

                        <React.Fragment>
                            <SelectValue basicType={fieldType}
                                         id={ruleIndex}
                                         index={whereIndex}
                                         mustValidate={true}
                                         onChange={setValue1}
                                         showValidationMessage={false}
                                         validationChecked={validationChecked}
                                         validateOnMount={false}
                                         value={typeof value1 !== "number" ? null : value1}
                            />

                            {isBetween &&
                                <React.Fragment>
                                    <span className={"mx-2 my-auto fs-13"}>and</span>
                                    <SelectValue basicType={fieldType}
                                                 id={ruleIndex}
                                                 index={whereIndex}
                                                 mustValidate={true}
                                                 onChange={setValue2}
                                                 showValidationMessage={false}
                                                 validateOnMount={false}
                                                 validationChecked={validationChecked}
                                                 value={typeof value2 !== "number" ? null : value2}
                                    />
                                </React.Fragment>
                            }
                        </React.Fragment>
                    }

                    {hasVariable && isTracBoolean(fieldType) && !isNullOperator &&
                        <SelectOption basicType={trac.BOOLEAN}
                                      id={ruleIndex}
                                      index={whereIndex}
                                      mustValidate={true}
                                      onChange={setValue1}
                                      options={booleanOptions}
                                      showValidationMessage={false}
                                      validateOnMount={false}
                                      validationChecked={validationChecked}
                                      value={operator}
                        />
                    }

                    {hasVariable && isTracDateOrDatetime(fieldType) && !isNullOperator &&
                        <React.Fragment>
                            <SelectDate basicType={fieldType}
                                        formatCode={isDateFormat(dateFormat) ? dateFormat : null}
                                        id={ruleIndex}
                                        index={whereIndex}
                                        mustValidate={true}
                                        onChange={setValue1}
                                        showValidationMessage={false}
                                        validationChecked={validationChecked}
                                        validateOnMount={false}
                                        value={typeof value1 !== "string" ? null : value1}
                            />
                            {isBetween &&
                                <React.Fragment>
                                    <span className={"mx-2 my-auto fs-13"}>and</span>
                                    <SelectDate basicType={fieldType}
                                                formatCode={isDateFormat(dateFormat) ? dateFormat : null}
                                                id={ruleIndex}
                                                index={whereIndex}
                                                mustValidate={true}
                                                onChange={setValue2}
                                                showValidationMessage={false}
                                                validationChecked={validationChecked}
                                                validateOnMount={false}
                                                value={typeof value2 !== "string" ? null : value2}
                                    />
                                </React.Fragment>
                            }
                        </React.Fragment>
                    }

                    <ConfirmButton ariaLabel={"Delete rule"}
                                   cancelText={"No"}
                                   className={"ms-3 px-0 d-none d-lg-block"}
                                   confirmText={"Yes"}
                                   description={"Are you sure that you want to delete this rule?"}
                                   dispatchedOnClick={deleteRule}
                                   id={ruleIndex}
                                   index={whereIndex}
                                   variant={"link"}
                    >
                        <Icon ariaLabel={false}
                              icon={'bi-trash3'}
                        />
                    </ConfirmButton>

                </Col>
            </Row>

        </React.Fragment>
    )
};

RuleInner.propTypes = {

    rule: PropTypes.object.isRequired,
    ruleIndex: PropTypes.number.isRequired,
    validationChecked: PropTypes.bool.isRequired,
    variableOptions: PropTypes.array.isRequired,
    whereIndex: PropTypes.number.isRequired
};

export const Rule =  memo(RuleInner);