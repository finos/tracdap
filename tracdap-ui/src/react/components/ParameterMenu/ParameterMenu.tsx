/**
 * A component that shows a grid of parameter options under different categories or in tabs. The tabbed version can not
 * be moved it its own component like the list version because only Tab components can be children to Tabs components.
 *
 * @module ParameterMenu
 * @category Component
 */

import {convertKeyToText} from "../../utils/utils_string";
import type {ListsOrTabs, Option, SelectOptionProps} from "../../../types/types_general";
import {isObject} from "../../utils/utils_trac_type_chckers";
import ParameterGrid from "./ParameterGrid";
import {ParameterGridInList} from "./ParameterGridInList";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {setParametersByCategory} from "../../utils/utils_attributes_and_parameters";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";
import {SelectPayload} from "../../../types/types_general";

/**
 * A function that determines what default tab key should be selected based on what parameter categories
 * are available.
 *
 * @param parametersByCategory - The part of the flow setup that lists parameter keys by their category.
 * @returns The name of the default tab if set
 */
function getDefaultTab(parametersByCategory: Record<string, string[]>): string | undefined {

    if (!isObject(parametersByCategory)) return undefined

    const allCategories = Object.keys(parametersByCategory)

    const nonStandardCategories = allCategories.filter(category => !["GENERAL", "ADVANCED", "DEBUG"].includes(category))

    if (allCategories.includes("GENERAL")) {

        return "GENERAL"

    } else if (nonStandardCategories.length > 0) {

        return nonStandardCategories[0]

    } else if (allCategories.includes("ADVANCED")) {

        return "ADVANCED"

    } else if (allCategories.includes("DEBUG")) {

        return "DEBUG"

    } else {

        return undefined
    }
}

/**
 * An interface for the props of the ParameterMenu component.
 */
export interface Props {

    /**
     * A grid width for the xl and xxl Bootstrap sizes, on larger screens you may want to show three parameters
     * a row (baseWidth = 4) but dataset option two per row (baseWidth = 6) as there is more info to show in the input.
     * This prop controls this. On smaller screens you are more hampered and width is not set by prop.
     */
    baseWidth?: number
    /**
     * The css class to apply to the outer dic, this allows additional styles to be added to the component.
     * defaultValue ''
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
     * The text to show along with the category name e.g. GeneralChartConfig attributes
     */
    label: "attributes" | "parameters" | "input datasets" | ""
    /**
     * The key of the last parameter changed by the user. This is a rendering optimisation that is needed
     * because when the user changes a parameter the whole parameter object updates and this triggers a
     * full rerender of the parameter menu. To get around this we memoize the ParameterGrid component so
     * that it only re-renders if the updated parameter is inside it. Other grids don't update. If set to null
     * then the optimisation is not used.
     */
    lastParameterChanged: null | string
    /**
     * How the parameters should be shown, in a list or in tabs.
     * @defaultValue 'lists'
     */
    listsOrTabs?: ListsOrTabs
    /**
     * Function to run when a parameter is changed
     */
    onChange: Function
    /**
     * The processed attributes/parameters object. This is the dataset from TRAC processed to have all the options,
     * default and minimum and maximum values set. This is created by the processAttributes function.
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
     * Whether to show the key for each option in the menu rather than the label. This is useful when trying to
     * load up datasets and get the keys of options that have not been set yet.
     * @defaultValue false
     */
    showKeysInsteadOfLabels?: boolean
    /**
     * A key that is attached to each parameter and returned with the onChange function and that references a key
     * in a store for what to update.
     */
    storeKey?: string
    /**
     * Whether the validation of the attribute/parameter values has been performed, for example if the user has tried
     * to kick of a model. When true the validation messages will show if there are any issues found.
     */
    validationChecked: boolean
    /**
     * The user set values of the attributes/parameters.
     */
    values:Record<string, SelectPayload<Option, boolean>["value"]>
}

export const ParameterMenu = (props: Props) => {

    const {
        baseWidth,
        className = "",
        hideDisabledOptions,
        isDisabled = false,
        label,
        lastParameterChanged,
        listsOrTabs = "lists",
        storeKey,
        onChange,
        parameters,
        policyValues,
        selectOptionObjectType,
        selectOptionOnCreateNewOption,
        showKeysInsteadOfLabels = false,
        values,
        validationChecked
    } = props

    // An object that contains a list of which parameters are in which categories
    const parametersByCategory = useMemo(() => setParametersByCategory(parameters), [parameters])

    // Work out what the default tab should be
    const defaultTab = getDefaultTab(parametersByCategory)

    // Is there more than one category with non-hidden parameters, if so we need to show headers
    const showHeaders = [...new Set(Object.values(parameters).filter(parameter => !parameter.hidden).map(parameter => parameter.category))].length > 1

    return (

        <React.Fragment>

            {listsOrTabs === "tabs" && Object.keys(parametersByCategory).length > 1 &&

                <div className={className}>
                    <Tabs defaultActiveKey={defaultTab} id={"parameter-options"}>

                        {parametersByCategory.hasOwnProperty("GENERAL") &&
                            <Tab className={"tab-content-bordered pt-5 pb-4"}
                                 eventKey={"GENERAL"}
                                 title={convertKeyToText("GENERAL")}
                            >
                                <ParameterGrid baseWidth={baseWidth}
                                               hideDisabledOptions={hideDisabledOptions}
                                               isDisabled={isDisabled}
                                               storeKey={storeKey}
                                               parameterKeyList={parametersByCategory["GENERAL"]}
                                               parameters={parameters}
                                               values={values}
                                               policyValues={policyValues}
                                               onChange={onChange}
                                               lastParameterChanged={lastParameterChanged}
                                               selectOptionObjectType={selectOptionObjectType}
                                               selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                                               validationChecked={validationChecked}
                                />
                            </Tab>
                        }

                        {Object.keys(parametersByCategory).filter(category => !["GENERAL", "ADVANCED", "DEBUG"].includes(category)).map(category =>
                            <Tab className={"tab-content-bordered pt-5 pb-4"}
                                 eventKey={category}
                                 key={category}
                                 title={convertKeyToText(category)}
                            >
                                <ParameterGrid baseWidth={baseWidth}
                                               hideDisabledOptions={hideDisabledOptions}
                                               isDisabled={isDisabled}
                                               storeKey={storeKey}
                                               key={category}
                                               parameterKeyList={parametersByCategory[category]}
                                               parameters={parameters}
                                               values={values}
                                               policyValues={policyValues}
                                               onChange={onChange}
                                               lastParameterChanged={lastParameterChanged}
                                               validationChecked={validationChecked}
                                               selectOptionObjectType={selectOptionObjectType}
                                               selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                                />
                            </Tab>
                        )}

                        {parametersByCategory.hasOwnProperty("ADVANCED") &&
                            <Tab eventKey={"ADVANCED"} title={convertKeyToText("ADVANCED")}
                                 className={"tab-content-bordered pt-5 pb-4"}>
                                <ParameterGrid baseWidth={baseWidth}
                                               hideDisabledOptions={hideDisabledOptions}
                                               isDisabled={isDisabled}
                                               storeKey={storeKey}
                                               parameterKeyList={parametersByCategory["ADVANCED"]}
                                               parameters={parameters}
                                               values={values}
                                               policyValues={policyValues}
                                               onChange={onChange}
                                               lastParameterChanged={lastParameterChanged}
                                               validationChecked={validationChecked}
                                               selectOptionObjectType={selectOptionObjectType}
                                               selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                                />
                            </Tab>
                        }

                        {parametersByCategory.hasOwnProperty("DEBUG") &&
                            <Tab eventKey={"DEBUG"} title={convertKeyToText("DEBUG")}
                                 className={"tab-content-bordered pt-5 pb-4"}>
                                <ParameterGrid baseWidth={baseWidth}
                                               hideDisabledOptions={hideDisabledOptions}
                                               isDisabled={isDisabled}
                                               storeKey={storeKey}
                                               parameterKeyList={parametersByCategory["DEBUG"]}
                                               parameters={parameters}
                                               values={values}
                                               policyValues={policyValues}
                                               onChange={onChange}
                                               lastParameterChanged={lastParameterChanged}
                                               validationChecked={validationChecked}
                                               selectOptionObjectType={selectOptionObjectType}
                                               selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                                />
                            </Tab>
                        }
                    </Tabs>
                </div>
            }

            {(listsOrTabs === "lists" || Object.keys(parametersByCategory).length < 2) &&
                <React.Fragment>
                    {parametersByCategory.hasOwnProperty("GENERAL") &&
                        <ParameterGridInList baseWidth={baseWidth}
                                             category={"GENERAL"}
                                             hideDisabledOptions={hideDisabledOptions}
                                             isDisabled={isDisabled}
                                             storeKey={storeKey}
                                             parameterKeyList={parametersByCategory["GENERAL"]}
                                             parameters={parameters}
                                             values={values}
                                             policyValues={policyValues}
                                             onChange={onChange}
                                             label={label}
                                             lastParameterChanged={lastParameterChanged}
                                             validationChecked={validationChecked}
                                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                                             showHeader={showHeaders}
                                             selectOptionObjectType={selectOptionObjectType}
                                             selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                        />
                    }

                    {Object.keys(parametersByCategory).filter(category => !["GENERAL", "ADVANCED", "DEBUG"].includes(category)).map(category =>
                        <ParameterGridInList baseWidth={baseWidth}
                                             key={category}
                                             hideDisabledOptions={hideDisabledOptions}
                                             isDisabled={isDisabled}
                                             category={category}
                                             storeKey={storeKey}
                                             parameterKeyList={parametersByCategory[category]}
                                             parameters={parameters}
                                             values={values}
                                             policyValues={policyValues}
                                             onChange={onChange}
                                             label={label}
                                             lastParameterChanged={lastParameterChanged}
                                             validationChecked={validationChecked}
                                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                                             showHeader={showHeaders}
                                             selectOptionObjectType={selectOptionObjectType}
                                             selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                        />
                    )}

                    {parametersByCategory.hasOwnProperty("ADVANCED") &&
                        <ParameterGridInList baseWidth={baseWidth}
                                             category={"ADVANCED"}
                                             storeKey={storeKey}
                                             hideDisabledOptions={hideDisabledOptions}
                                             isDisabled={isDisabled}
                                             parameterKeyList={parametersByCategory["ADVANCED"]}
                                             parameters={parameters}
                                             values={values}
                                             policyValues={policyValues}
                                             onChange={onChange}
                                             label={label}
                                             lastParameterChanged={lastParameterChanged}
                                             validationChecked={validationChecked}
                                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                                             showHeader={showHeaders}
                                             selectOptionObjectType={selectOptionObjectType}
                                             selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                        />
                    }

                    {parametersByCategory.hasOwnProperty("DEBUG") &&
                        <ParameterGridInList baseWidth={baseWidth}
                                             category={"DEBUG"}
                                             hideDisabledOptions={hideDisabledOptions}
                                             isDisabled={isDisabled}
                                             storeKey={storeKey}
                                             parameterKeyList={parametersByCategory["DEBUG"]}
                                             parameters={parameters}
                                             values={values}
                                             policyValues={policyValues}
                                             onChange={onChange}
                                             label={label}
                                             lastParameterChanged={lastParameterChanged}
                                             validationChecked={validationChecked}
                                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                                             showHeader={showHeaders}
                                             selectOptionObjectType={selectOptionObjectType}
                                             selectOptionOnCreateNewOption={selectOptionOnCreateNewOption}
                        />
                    }

                </React.Fragment>
            }
        </React.Fragment>
    )
};

ParameterMenu.propTypes = {

    baseWidth: PropTypes.number,
    className: PropTypes.string,
    listsOrTabs: PropTypes.oneOf(["lists", "tabs"]),
    onChange: PropTypes.func.isRequired,
    isDisabled: PropTypes.bool,
    parameters: PropTypes.objectOf(
        //TODO check this against the type
        PropTypes.shape({
            basicType: PropTypes.number.isRequired,
            specialType: PropTypes.string,
            name: PropTypes.string.isRequired,
            description: PropTypes.string,
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
            category: PropTypes.string,
            minimumValue: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
            maximumValue: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
            tooltip: PropTypes.string,
            hidden: PropTypes.bool,
            isMulti: PropTypes.bool,
            mustValidate: PropTypes.bool.isRequired,
            rows: PropTypes.number,
            widthMultiplier: PropTypes.number,
        })),
    values: PropTypes.objectOf(
        PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.bool, PropTypes.object, PropTypes.array])
    ),
    parametersByCategory: PropTypes.objectOf(
        PropTypes.arrayOf(PropTypes.string)
    ),
    label: PropTypes.oneOf(["parameters", "attributes", "input datasets", ""]).isRequired,
    lastParameterChanged: PropTypes.string,
    validationChecked: PropTypes.bool,

    selectOptionObjectType: PropTypes.number,
    selectOptionOnCreateNewOption: PropTypes.func
};