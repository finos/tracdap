/**
 * A component that shows a table listing the models selected for a job.
 *
 * @module ModelListTable
 * @category Component
 */

import {areSelectValuesEqual} from "../utils/utils_attributes_and_parameters";
import Badge from "react-bootstrap/Badge";
import {Burger} from "./Burger";
import {Button} from "./Button";
import type {ButtonPayload, SearchOption, SelectValuePayload} from "../../types/types_general";
import {checkProperties, convertSearchResultsIntoOptions, enrichProperties} from "../utils/utils_trac_metadata";
import Dropdown from "react-bootstrap/Dropdown";
import {hasOwnProperty, isPrimitive, isTagOption} from "../utils/utils_trac_type_chckers";
import {Icon} from "./Icon";
import {ModelInfoModal} from "./ModelInfoModal";
import {PolicyStatusIcon} from "./PolicyStatusIcon";
import PropTypes from "prop-types";
import React, {useCallback, useMemo, useState} from "react";
import {SelectValue} from "./SelectValue";
import type {SingleValue} from "react-select";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

// The properties from the metadata tag to add to the table. If you change this make
// sure you update the array references in the code below.
const propertiesToShow: { tag: "attrs" | "header", property: string }[] = [
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
    selectedModel: {key: null, tagHeader: null},
    showInfoModal: false,
    showKeysInsteadOfLabels: false
}

/**
 * An interface for the props of the ModelListTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The metadata for the models used in the job.
     */
    models?: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>
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
    /**
     * Whether to show a header row in the table.
     */
    showHeader?: boolean
}

/**
 * An interface for the state of the ModelListTable component.
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
     * What model has been selected in the table to view the full information for in a modal.
     */
    selectedModel: { key: string | null, tagHeader: null | trac.metadata.ITagHeader }
    /**
     * Whether to show the model info modal.
     */
    showInfoModal: boolean
    /**
     * Whether to show the key in the flow rather than the selected option's name.
     */
    showKeysInsteadOfLabels: boolean
}

export const ModelListTable = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        models,
        overlayCount,
        policyValues,
        showHeader = true
    } = props;

    // Get what we need from the store
    const {allProcessedAttributes} = useAppSelector(state => state["setAttributesStore"])

    // A hook to manage the local state of the component
    const [{burgerOpen, search, selectedModel, showInfoModal, showKeysInsteadOfLabels}, setState] = useState<State>(initialState)

    /**
     * A function that runs when the user changes the search box value. The useCallback means that
     * a new function is not created each render.
     */
    const onChangeInput = useCallback((payload: SelectValuePayload<string>) => {

        setState(prevState => ({...prevState, search: payload.value}))

    }, [])

    // The payload is void when the modal is closed
    const toggleModelInfo = (payload: ButtonPayload | void) => {

        let tagHeader: null | trac.metadata.ITagHeader = null
        let key: null | string = null

        // e will not exist when the modal is closed - id is the modelKey
        if (payload) {

            const {id} = payload

            if (typeof id === "string" && models !== undefined) {
                const model = models[id]
                tagHeader = (isTagOption(model) ? model.tag.header : model?.header) ?? null
                key = id
            }
        }

        setState(prevState => ({...prevState, showInfoModal: !prevState.showInfoModal, selectedModel: {key, tagHeader}}))
    }

    /**
     * A function that filters the model entries by the search term entered by the user. The result is memoized so that
     * it only ever runs if either the models or the search term changes. The model list is also recast to an array
     * and has some other details added that make it easier to render the table and do less processing outside the
     * useMemo hook.
     */
    const filteredModelList: { key: string, tag: trac.metadata.ITag, isValueAlignedWithPolicy: undefined | boolean, finalPropertiesToShow: ReturnType<typeof enrichProperties> }[] = useMemo(() =>

            Object.entries(models || {}).map(([key, model]) => {

                const tag = isTagOption(model) ? model.tag : model

                // Work out which of the properties requested actually exist
                const finalPropertiesToShow = enrichProperties(checkProperties(propertiesToShow, tag), tag, allProcessedAttributes)

                // The model header converted to an option, this is needed as an argument to the areAttributeValuesEqual function
                const modelOption: SearchOption[] = convertSearchResultsIntoOptions([{header: tag.header}], false, false, false, false, false)

                // If a policy value is set then work out if the selected value matches the required policy value
                const isValueAlignedWithPolicy = hasOwnProperty(policyValues, key) ? areSelectValuesEqual(policyValues[key], modelOption[0]) : undefined

                return {key, tag, isValueAlignedWithPolicy, finalPropertiesToShow}

            }).filter((item) => {

                // If there is a search term then get those models that match the search term
                return search == null ||
                    (item.tag.header != undefined && item.tag.header.objectId.toUpperCase().includes(search.toUpperCase())) ||
                    item.key.toUpperCase().includes(search.toUpperCase()) ||
                    // Index 0 is the name property
                    (typeof item.finalPropertiesToShow[0].value === "string" && item.finalPropertiesToShow[0].value.toUpperCase().includes(search.toUpperCase()))
            })

        , [allProcessedAttributes, models, policyValues, search])

    return (

        <React.Fragment>

            {(!models || Object.keys(models).length === 0) &&
                <div className={"pb-1"}>There are no models</div>
            }

            {models && Object.keys(models).length > 0 &&

                <React.Fragment>
                    <div className={`${className} d-flex mb-3 align-items-center justify-content-end`}>

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
                                         labelText={"Search models:"}
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

                    <Table responsive className={`dataHtmlTable`}>

                        {showHeader &&
                            <thead>
                            <tr>
                                <th>{showKeysInsteadOfLabels ? "Key" : "Name"}</th>
                                <th>Object ID</th>
                                <th className={"text-center"}>Object version</th>
                                <th className={"text-center"}>Last update</th>
                                <th>Updated by</th>
                                <th className={"text-center"}>Overlays</th>
                                {policyValues &&
                                    <th className={"text-center"}>Policy compliance</th>
                                }
                                <th className={"text-center"}>More info</th>
                            </tr>
                            </thead>
                        }

                        <tbody>

                        {filteredModelList.length === 0 &&
                            <tr>
                                <td colSpan={7 + (policyValues ? 1 : 0)} className={"text-center"}>There are no search results</td>
                            </tr>
                        }

                        {filteredModelList.map(item => {

                            return (
                                <tr key={item.key}>
                                    <td>
                                        {isPrimitive(item.finalPropertiesToShow[0]?.value) ? showKeysInsteadOfLabels ? item.key : item.finalPropertiesToShow[0]?.value || item.key : null}
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

                                    <td className={"text-center"}>
                                        {hasOwnProperty(overlayCount, item.key) && typeof overlayCount[item.key] === "number" ?
                                            <Badge bg={overlayCount[item.key] > 0 ? "secondary" : "light"}>
                                                {overlayCount[item.key]}
                                            </Badge>
                                            : "Unavailable"}
                                    </td>

                                    {policyValues &&
                                        <td className={"text-center"}>
                                            <PolicyStatusIcon coveredInPolicy={hasOwnProperty(policyValues, item.key)}
                                                              isValueAlignedWithPolicy={item.isValueAlignedWithPolicy}
                                            />
                                        </td>
                                    }

                                    <td className={"text-center"}>
                                        <Button ariaLabel={"Show model info"}
                                                className={"min-width-px-60 py-0"}
                                                id={item.key}
                                                isDispatched={false}
                                                onClick={toggleModelInfo}
                                                variant={"info"}
                                        >
                                            <Icon ariaLabel={false}
                                                  icon={"bi-info-circle"}
                                            />
                                        </Button>
                                    </td>
                                </tr>
                            )
                        })}

                        </tbody>
                    </Table>
                </React.Fragment>
            }

            <ModelInfoModal show={showInfoModal}
                            tagSelector={selectedModel.tagHeader}
                            toggle={toggleModelInfo}
            />

        </React.Fragment>
    )
};

ModelListTable.propTypes = {

    className: PropTypes.string,
    models: PropTypes.object,
    overlayCount: PropTypes.objectOf(PropTypes.number),
    policyValues: PropTypes.object,
    showHeader: PropTypes.bool
};