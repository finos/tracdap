/**
 * A component that allows the user to create an SQL query statement to run on a dataset.
 *
 * @module Select
 * @category Component
 */

import {addToSelect, moveInHistory, type QueryBuilderStoreState, setEndGroup, setFieldTypeButton, setFunctionButton, setOperatorButton, setSelectVariable} from "./queryBuilderStore";
import {Button} from "../Button";
import Col from "react-bootstrap/Col";
import {configuration} from "./config_query_builder";
import {convertSchemaToOptions} from "../../utils/utils_schema";
import ErrorBoundary from "../ErrorBoundary";
import {FieldTypeSelector} from "../FieldTypeSelector";
import {type FilterOptionOption} from "react-select/dist/declarations/src/filters";
import {FunctionButton} from "./FunctionButton";
import {hasOwnProperty, isTracBasicType} from "../../utils/utils_trac_type_chckers";
import {HeaderTitle} from "../HeaderTitle";
import {OperatorButton} from "./OperatorButton";
import type {Option} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Select component.
 */
export interface Props {

    /**
     * The object key of the data the user has selected to query, the key include the object version. The
     * store has a separate area for each dataset that the user has loaded up into QueryBuilder.
     */
    objectKey: string
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof QueryBuilderStoreState["uses"]
}

export const Select = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {
        added,
        aggregation,
        fieldTypes,
        operator,
        sqlFunction,
        unique,
        variable,
        validationChecked
    } = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].selectTab)

    // Get the list of options we need
    const {aggregationButtons, operatorButtons, numberButtons, textButtons, datetimeButtons, uniqueButtons} = configuration

    const {schema} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].inputData)

    // The term in the select statement that is being edited, so select A, B, A*B+C as D has three terms, and
    // we are always editing the last one. This is needed to work out whether to end the A*B+C part (called
    // a group) and move on to a fourth term.
    const selectToEdit = added.length > 0 ? added[added.length - 1] : []

    // Get the history of the UI for the dataset if set
    const {indexLoaded, positions} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].history[objectKey])

    // The schema converted to a set of options
    const variableOptions = useMemo(() => convertSchemaToOptions(schema || [], true, false, false), [schema])

    // The fieldType with a fallback
    const fieldType = variable?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET

    /**
     * A function that is passed to the SelectOption component to use to filter the options based on what field types the user
     * has asked to show. The useCallback is an optimisation to prevent unnecessary re-renders. By passing the function to
     * do the filtering in the SelectOption component we avoid re-renders in the parent component.
     * @param option - The option in the select.
     * @param inputValue - The value of the search box in the select.
     */
    const selectFilterOption = useCallback((option: FilterOptionOption<Option>, inputValue: string): boolean => {

        const {label, value} = option

        const optionFieldType = hasOwnProperty(option.data, "details") && hasOwnProperty(option.data.details, "schema") && hasOwnProperty(option.data.details.schema, "fieldType") && isTracBasicType(option.data?.details?.schema?.fieldType) && option.data?.details?.schema?.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET
        return Boolean(fieldTypes && fieldTypes.includes(optionFieldType) && (inputValue === "" || label.toUpperCase().includes(inputValue.toUpperCase()) || value.toUpperCase().includes(inputValue.toUpperCase())))

    }, [fieldTypes])

    // TODO add guide using tooltips

    return (

        <ErrorBoundary>
            <Row>
                <Col md={2}>
                    <div className={"d-flex justify-content-between flex-column h-100"}>
                        <HeaderTitle text={"Aggregation"} type={"h5"}/>

                        {aggregationButtons.map(button =>
                            <FunctionButton disabled={!button.fieldTypes.includes(fieldType)}
                                            id={"aggregation"}
                                            key={button.value}
                                            label={button.label}
                                            name={button.value}
                                            onClick={setFunctionButton}
                                            selected={aggregation?.value === button.value}
                            />
                        )}
                        <HeaderTitle text={"Text"} type={"h5"}/>

                        {textButtons.map(button =>
                            <FunctionButton disabled={!button.fieldTypes.includes(fieldType)}
                                            id={"sqlFunction"}
                                            key={button.value}
                                            label={button.label}
                                            name={button.value}
                                            onClick={setFunctionButton}
                                            selected={sqlFunction?.value === button.value}
                            />
                        )}
                    </div>
                </Col>
                <Col md={2}>
                    <div className={"d-flex justify-content-between flex-column h-100"}>
                        <HeaderTitle text={"Number"} type={"h5"}/>

                        {numberButtons.map(button =>
                            <FunctionButton disabled={!button.fieldTypes.includes(fieldType)}
                                            id={"sqlFunction"}
                                            key={button.value}
                                            label={button.label}
                                            name={button.value}
                                            onClick={setFunctionButton}
                                            selected={sqlFunction?.value === button.value}
                            />
                        )}

                        <HeaderTitle text={"Datetime"} type={"h5"}/>

                        {datetimeButtons.map(button =>
                            <FunctionButton disabled={!button.fieldTypes.includes(fieldType)}
                                            id={"sqlFunction"}
                                            key={button.value}
                                            label={button.label}
                                            name={button.value}
                                            onClick={setFunctionButton}
                                            selected={sqlFunction?.value === button.value}
                            />
                        )}

                        <HeaderTitle text={"Unique"} type={"h5"}/>

                        {uniqueButtons.map(button =>
                            <FunctionButton id={"unique"}
                                            key={button.value}
                                            label={button.label}
                                            name={button.value}
                                            onClick={setFunctionButton}
                                            selected={unique?.value === button.value}
                            />
                        )}
                    </div>
                </Col>
                <Col md={8}>
                    <div className={"d-flex justify-content-between flex-column h-100"}>
                        <HeaderTitle text={"Filter the variable list"} type={"h5"}/>

                        <div className={"d-grid d-md-flex"}>
                            <FieldTypeSelector fieldTypes={fieldTypes} dispatchedOnClick={setFieldTypeButton}/>
                        </div>

                        <HeaderTitle text={"Variable list"} type={"h5"}/>

                        <Row>
                            <Col xs={12} className={"mb-3 mt-2"}>

                                Please select a variable you want to add to the query. If you want to apply
                                an aggregation and/or a function to the variable then select the functions
                                before adding it.

                            </Col>

                            <Col xs={12} md={12} lg={8} xl={8}>
                                <SelectOption basicType={trac.STRING}
                                              filterOption={selectFilterOption}
                                              isClearable={true}
                                              mustValidate={true}
                                              onChange={setSelectVariable}
                                              options={variableOptions}
                                              showValidationMessage={true}
                                              validationChecked={validationChecked}
                                              value={variable}
                                />
                            </Col>

                            <Col xs={12} md={12} lg={4} xl={4} className={"mt-3 mt-lg-0"}>
                                <Button ariaLabel={"Add select"}
                                        className={"ms-0 ms-lg-2"}
                                        isDispatched={true}
                                        onClick={addToSelect}
                                        variant={"secondary"}

                                >
                                    Add
                                </Button>
                                <Button ariaLabel={"Add select"}
                                        className={"ms-2"}
                                        disabled={indexLoaded === 0}
                                        id={-1}
                                        isDispatched={true}
                                        onClick={moveInHistory}
                                        variant={"secondary"}
                                >
                                    Undo
                                </Button>
                                <Button ariaLabel={"Redo"}
                                        className={"ms-2"}
                                        disabled={indexLoaded >= positions.length - 1}
                                        id={1}
                                        isDispatched={true}
                                        onClick={moveInHistory}
                                        variant={"secondary"}
                                >
                                    Redo
                                </Button>
                            </Col>
                        </Row>

                        {/*Smaller top margin due to SelectOption validation message space*/}
                        <HeaderTitle text={"Operators"} type={"h5"} outerClassName={"mt-2 mb-3"}/>

                        <Row>
                            <Col xs={12} className={"mb-3 mt-2"}>

                                Operators are used to group variables together into one term, for example
                                to add two columns together. Operators only work with numeric variables.
                                To stop adding terms to a group use the &apos;End group&apos; button.

                            </Col>

                            <Col xs={12}>
                                {operatorButtons.map(button =>

                                    <OperatorButton key={button.value}
                                                    name={button.value}
                                                    selected={operator?.value === button.value}
                                                    icon={button.icon}
                                                    onClick={setOperatorButton}
                                    />
                                )}


                                <Button ariaLabel={"End group"}
                                        className={`ms-4 mb-3 mb-md-2 ${selectToEdit.length < 2 || selectToEdit[selectToEdit.length - 1].isLastOfGroup ? "d-none" : ""}`}
                                        disabled={selectToEdit.length < 2 || selectToEdit[selectToEdit.length - 1].isLastOfGroup}
                                        isDispatched={true}
                                        onClick={setEndGroup}
                                        size={"sm"}
                                        variant={"secondary"}
                                >
                                    End group
                                </Button>

                            </Col>
                        </Row>
                    </div>
                </Col>
            </Row>
        </ErrorBoundary>
    )
};

Select.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};