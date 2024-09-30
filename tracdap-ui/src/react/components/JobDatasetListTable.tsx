/**
 * A component that shows a table listing the input datasets for a flow or input/output datasets for a job.
 *
 * @module JobDatasetListTable
 * @category Component
 */

import {areSelectValuesEqual} from "../utils/utils_attributes_and_parameters";
import Badge from "react-bootstrap/Badge";
import {Burger} from "./Burger";
import {Button} from "./Button";
import type {ButtonPayload, SearchOption, SelectValuePayload} from "../../types/types_general";
import {checkProperties, convertSearchResultsIntoOptions, enrichProperties} from "../utils/utils_trac_metadata";
import {DataInfoModal} from "./DataInfoModal";
import Dropdown from "react-bootstrap/Dropdown";
import {hasOwnProperty, isPrimitive, isTagOption} from "../utils/utils_trac_type_chckers";
import {Icon} from "./Icon";
import {PolicyStatusIcon} from "./PolicyStatusIcon";
import PropTypes from "prop-types";
import React, {useCallback, useMemo, useState} from "react";
import {SelectValue} from "./SelectValue";
import type {SingleValue} from "react-select";
import {sortArrayBy} from "../utils/utils_arrays";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

// The data from the tag that we need to extract to show it in the table
const propertiesToShow: { tag: "attrs" | "header" | string[], property: string }[] = [
    {tag: "attrs", property: "name"},
    {tag: "header", property: "objectId"},
    {tag: "header", property: "objectVersion"},
    {tag: "header", property: "objectTimestamp"},
    {tag: "attrs", property: "trac_update_user_name"}
]

// The initial state of the component
const initialState: State = {
    burgerOpen: false,
    search: null,
    selectedDataset: {key: null, tagHeader: null},
    showInfoModal: false,
    showKeysInsteadOfLabels: false
}

/**
 * An interface for the props of the JobDatasetListTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The metadata for the datasets selected.
     */
    datasets: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>
    /**
     * The datasets in the datasets prop that we should list, for example if type is required, then this list if the
     * keys of all the required datasets.
     */
    inputsForCategory?: string[]
    /**
     * Whether to show a header row in the table.
     */
    showHeader?: boolean
    /**
     * The number of overlays applied to the outputs of the model.
     */
    overlayCount?: Record<string, number>
    /**
     * The values of the models that are considered in policy, when the user selects values equal to
     * these we show additional feedback that the values match these. This is used for example
     * when re-running a job.
     */
    policyValues?: Record<string, SingleValue<SearchOption>>
}

/**
 * An interface for the state of the JobDatasetListTable component.
 */
export interface State {

    /**
     * Whether the burger sub menu is open.
     */
    burgerOpen: boolean
    /**
     * The user set search term for filtering the table.
     */
    search: null | string
    /**
     * What dataset has been selected in the table to view the full information for in a modal.
     */
    selectedDataset: { key: string | null, tagHeader: null | trac.metadata.ITagHeader }
    /**
     * Whether to show the dataset info modal.
     */
    showInfoModal: boolean
    /**
     * Whether to show the key in the flow/job rather than the selected option's name.
     */
    showKeysInsteadOfLabels: boolean
}

export const JobDatasetListTable = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        datasets,
        inputsForCategory = [],
        overlayCount,
        policyValues,
        showHeader = true
    } = props;

    // Get what we need from the store
    const {allProcessedAttributes} = useAppSelector(state => state["setAttributesStore"])

    // A hook to manage the local state of the component
    const [{burgerOpen, search, selectedDataset, showInfoModal, showKeysInsteadOfLabels}, setState] = useState<State>(initialState)

    /**
     * A function that runs when the user changes the search box value. The useCallback means that
     * a new function is not created each render.
     */
    const onChangeInput = useCallback((payload: SelectValuePayload<string>) => {

        setState(prevState => ({...prevState, search: payload.value}))

    }, [])

    // The payload is void when the modal is closed
    const toggleDataInfo = (payload: ButtonPayload | void) => {

        let tagHeader: null | trac.metadata.ITagHeader = null
        let key: null | string = null

        // e will not exist when the modal is closed - id is the modelKey
        if (payload) {

            const {id} = payload

            if (typeof id === "string" && datasets !== undefined) {
                const data = datasets[id]
                tagHeader = (isTagOption(data) ? data.tag.header : data?.header) ?? null
                key = id
            }
        }

        setState(prevState => ({...prevState, showInfoModal: !prevState.showInfoModal, selectedDataset: {key, tagHeader}}))
    }

    /**
     * A function that filters the dataset entries by the search term entered by the user. The result is memoized so that
     * it only ever runs if either the datasets or the search term changes. The dataset list is also recast to an array
     * and has some other details added that make it easier to render the table and do less processing outside the
     * useMemo hook. Note that the array is also sorted.
     */
    const filteredDatasetList: { key: string, name: string, tag: null | trac.metadata.ITag, isValueAlignedWithPolicy: undefined | boolean, finalPropertiesToShow: ReturnType<typeof enrichProperties> }[] = useMemo(() =>

            sortArrayBy(
                Object.entries(datasets || {}).map(([key, dataset]) => {

                    // tag will be null for optional inputs that are not selected
                    const tag: null | trac.metadata.ITag = isTagOption(dataset) ? dataset.tag : dataset

                    if (tag) {
                        // Work out which of the properties requested actually exist
                        const finalPropertiesToShow = enrichProperties(checkProperties(propertiesToShow, tag), tag, allProcessedAttributes)

                        // The model header converted to an option, this is needed as an argument to the areAttributeValuesEqual function
                        const modelOption: SearchOption[] = convertSearchResultsIntoOptions([{header: tag?.header}], false, false, false, false, false)

                        // If a policy value is set then work out if the selected value matches the required policy value
                        const isValueAlignedWithPolicy = hasOwnProperty(policyValues, key) ? areSelectValuesEqual(policyValues[key], modelOption[0]) : undefined

                        return {key, name: typeof finalPropertiesToShow[0]?.value === "string" ? finalPropertiesToShow[0]?.value : key, tag, isValueAlignedWithPolicy, finalPropertiesToShow}

                    } else {
                        return {key, name: key, tag: null, isValueAlignedWithPolicy: hasOwnProperty(policyValues, key) && policyValues[key] === null, finalPropertiesToShow: []}
                    }

                }).filter((item) => {

                    // If there is a search term then get those models that match the search term
                    return (
                        (inputsForCategory.length === 0 || inputsForCategory.includes(item.key)) && (
                            search == null ||
                            (item.tag?.header != undefined && item.tag.header.objectId?.toUpperCase().includes(search.toUpperCase())) ||
                            item.key.toUpperCase().includes(search.toUpperCase()) ||
                            item.name.toUpperCase().includes(search.toUpperCase())
                        )
                    )
                }), "name")

        , [allProcessedAttributes, datasets, inputsForCategory, policyValues, search])

    return (

        <React.Fragment>

            {Object.keys(datasets).length === 0 &&
                <div className={"pb-1"}>There are no datasets</div>
            }

            {Object.keys(datasets).length > 0 &&
                <div className={className}>

                    <div className={`d-flex mb-3 align-items-center justify-content-end`}>

                        {/*This puts any children on the same row as the search widget but on the left*/}
                        {children &&
                            <div className={"flex-fill"}>
                                {children}
                            </div>
                        }

                        <div className={`${children ? "flex-fill" : "w-100 w-md-50 w-xxl-33 d-flex"}`}>
                            <SelectValue onChange={onChangeInput}
                                         basicType={trac.STRING}
                                         className={"flex-fill"}
                                         labelPosition={"left"}
                                         labelText={"Search datasets:"}
                                         isDispatched={false}
                                         value={search}
                                         validateOnMount={false}
                                         showValidationMessage={false}
                                         mustValidate={false}
                            />

                            <Dropdown className={`ms-2`}
                                      drop={"up"}
                                      onToggle={isOpen => setState(prevState => ({...prevState, burgerOpen: isOpen}))}
                            >
                                <Dropdown.Toggle bsPrefix={"p-0"}
                                                 className={`no-halo`}
                                                 id={"object-menu-button"}
                                                 size={"sm"}
                                                 title={"Dropdown button"}
                                                 variant={"link"}
                                >
                                    <Burger ariaLabel={"Menu icon"} open={burgerOpen} size={"md"}/>
                                </Dropdown.Toggle>

                                <Dropdown.Menu className={"py-2"}>
                                    <Dropdown.Item className={"fs-8 px-3"}
                                                   onClick={() => setState(prevState => ({...prevState, showKeysInsteadOfLabels: !prevState.showKeysInsteadOfLabels}))}
                                                   size={"sm"}
                                    >
                                        <Icon ariaLabel={false}
                                              className={"pe-2"}
                                              icon={showKeysInsteadOfLabels ? "bi-type" : "bi-link"}
                                        />
                                        <span>Show {showKeysInsteadOfLabels ? "labels" : "keys"}</span>
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>

                        </div>
                    </div>
                    <Table responsive className={` dataHtmlTable`}>

                        {showHeader &&
                            <thead>
                            <tr>
                                <th>{showKeysInsteadOfLabels ? "Key" : "Name"}</th>
                                <th>Object ID</th>
                                <th className={"text-center"}>Object version</th>
                                <th className={"text-center"}>Last update</th>
                                <th>Updated by</th>
                                {overlayCount &&
                                    <th className={"text-center"}>Overlays</th>
                                }
                                {policyValues &&
                                    <th className={"text-center"}>Policy compliance</th>
                                }
                                <th className={"text-center"}>More info</th>
                            </tr>
                            </thead>
                        }

                        <tbody>

                        {filteredDatasetList.length === 0 &&
                            <tr>
                                <td colSpan={7 + (policyValues ? 1 : 0) + (overlayCount ? 1 : 0)} className={"text-center"}>There are no search results</td>
                            </tr>
                        }

                        {filteredDatasetList.map(item => {

                            return (
                                <tr key={item.key}>

                                    {item.tag === null &&
                                        <React.Fragment>
                                            <td>
                                                {item.key}
                                            </td>
                                            <td colSpan={4 + (overlayCount ? 1 : 0)} className={"text-center"}>
                                                No dataset selected
                                            </td>
                                            {policyValues &&
                                                <td className={"text-center"}>
                                                    <PolicyStatusIcon coveredInPolicy={hasOwnProperty(policyValues, item.key)}
                                                                      isValueAlignedWithPolicy={item.isValueAlignedWithPolicy}
                                                    />
                                                </td>
                                            }
                                            <td className={"text-center"}>
                                            </td>
                                        </React.Fragment>
                                    }

                                    {item.tag !== null &&
                                        <React.Fragment>
                                            <td>
                                                {showKeysInsteadOfLabels ? item.key : item.name}
                                            </td>
                                            <td>
                                                <span className={"user-select-all"}>
                                                    {isPrimitive(item.finalPropertiesToShow[1]?.value) ? item.finalPropertiesToShow[1]?.value || "Not set" : null}
                                                </span>
                                            </td>
                                            <td className={"text-nowrap text-center"}>
                                                {isPrimitive(item.finalPropertiesToShow[2]?.value) ? item.finalPropertiesToShow[2]?.value || "Not set" : null}
                                            </td>
                                            <td className={"text-nowrap text-center"}>
                                                {isPrimitive(item.finalPropertiesToShow[3]?.value) ? item.finalPropertiesToShow[3]?.value || "Not set" : null}
                                            </td>
                                            <td>
                                                {isPrimitive(item.finalPropertiesToShow[4]?.value) ? item.finalPropertiesToShow[4]?.value || "Not set" : null}
                                            </td>

                                            {overlayCount &&
                                                <td className={"text-center"}>
                                                    {hasOwnProperty(overlayCount, item.key) && typeof overlayCount[item.key] === "number" ?
                                                        <Badge bg={overlayCount[item.key] > 0 ? "secondary" : "light"}>
                                                            {overlayCount[item.key]}
                                                        </Badge>
                                                        : "Unavailable"}
                                                </td>
                                            }

                                            {policyValues &&
                                                <td className={"text-center"}>
                                                    <PolicyStatusIcon coveredInPolicy={hasOwnProperty(policyValues, item.key)}
                                                                      isValueAlignedWithPolicy={item.isValueAlignedWithPolicy}
                                                    />
                                                </td>
                                            }

                                            <td className={"text-center"}>
                                                <Button ariaLabel={"Show dataset info"}
                                                        className={"min-width-px-60 py-0"}
                                                        id={item.key}
                                                        isDispatched={false}
                                                        onClick={toggleDataInfo}
                                                        variant={"info"}
                                                >
                                                    <Icon ariaLabel={false}
                                                          icon={"bi-info-circle"}
                                                    />
                                                </Button>
                                            </td>
                                        </React.Fragment>

                                    }
                                </tr>
                            )

                        })}

                        </tbody>
                    </Table>
                </div>
            }

            <DataInfoModal show={showInfoModal}
                           toggle={toggleDataInfo}
                           tagSelector={selectedDataset.tagHeader}
            />

        </React.Fragment>
    )
};

JobDatasetListTable.propTypes = {

    className: PropTypes.string,
    datasets: PropTypes.object,
    inputsForCategory: PropTypes.arrayOf(PropTypes.string),
    overlayCount: PropTypes.objectOf(PropTypes.number),
    policyValues: PropTypes.object,
    showHeader: PropTypes.bool
};