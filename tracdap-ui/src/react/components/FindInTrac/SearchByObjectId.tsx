/**
 * A component that shows a menu that allows the user to paste a set of object IDs or keys into the browser and load
 * those into their search results table.
 *
 * @module SearchByObjectId
 * @category Component
 */

import {
    addManyObjectIdSearchResultsToTable,
    addOneObjectIdSearchResultToTable,
    type FindInTracStoreState,
    setManyObjectIdSearchValue,
    setObjectIdObjectTypeFilter,
    setObjectIdSearchStatus
} from "./findInTracStore";
import Col from "react-bootstrap/Col";
import {convertObjectTypeToString} from "../../utils/utils_trac_metadata";
import {isObject} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {memo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import {SelectValue} from "../SelectValue";
import {Types} from "../../../config/config_trac_classifications";
import {TextBlock} from "../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

// A dummy function set as a variable to prevent component re-renders
const dummy = () => {
}

/**
 * An interface for the props of the SearchByObjectId component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

const SearchByObjectIdInner = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {objectType, oneOrMany, searchValue} = useAppSelector(state => state["findInTracStore"].uses[storeKey].searchByObjectId)

    const {tooltips} = useAppSelector(state => state["findInTracStore"].resultsSettings)

    const {
        filtersToShow,
    } = useAppSelector(state => state["findInTracStore"].uses[storeKey].filters)

    const objectTypeAsString = `${objectType?.type ? convertObjectTypeToString(objectType.type, true, true) : "object"}`

    return (
        <React.Fragment>

            <TextBlock>
                You can opt to search by {objectTypeAsString} ID or key, enter either in the search box below
                and if it s a valid ID or key the results will be shown in the table.
            </TextBlock>

            <Row>
                <Col xs={12} lg={3}>

                    <SelectOption basicType={trac.STRING}
                                  labelText={"Object type:"}
                                  mustValidate={false}
                                  name={"objectType"}
                                  onChange={setObjectIdObjectTypeFilter}
                                  options={Types.tracObjectTypes}
                                  showValidationMessage={true}
                                  storeKey={storeKey}
                                  tooltip={tooltips.objectType}
                                  validateOnMount={false}
                                  value={objectType}
                                  isDisabled={Boolean(!filtersToShow.includes("objectType"))}
                    />
                </Col>

                <Col xs={12} lg={oneOrMany === "one" ? 6 : 12}>

                    {/*If searches can only be made one at a time then use the SelectOption component*/}
                    {oneOrMany === "one" &&

                        <SelectOption basicType={trac.STRING}
                                      isClearable={false}
                                      isCreatable={true}
                                      isDispatched={false}
                                      labelText={"Search:"}
                                      mustValidate={false}
                                      objectType={objectType?.type}
                                      onChange={dummy}
                                      onCreateNewOption={addOneObjectIdSearchResultToTable}
                                      options={undefined}
                                      placeHolderText={`Please enter a valid ${objectTypeAsString} ID or key and press enter`}
                                      value={isObject(searchValue) ? searchValue : null}
                                      showValidationMessage={false}
                                      storeKey={storeKey}
                                      validationChecked={false}
                                      validateOnMount={false}
                        />
                    }

                    {/*If multiple searches can be made in one go then use the SelectValue component in text area mode */}
                    {oneOrMany === "many" &&

                        <SelectValue basicType={trac.STRING}
                                     getTagsFromValue={addManyObjectIdSearchResultsToTable}
                                     runningGetTagsFromValue={setObjectIdSearchStatus}
                                     isDispatched={true}
                                     labelText={"Search:"}
                                     mustValidate={false}
                                     objectType={objectType?.type}
                                     onChange={setManyObjectIdSearchValue}
                                     placeHolderText={`Please enter a valid ${objectTypeAsString} IDs or keys separated by commas or spaces, and press enter`}
                                     rows={5}
                                     showValidationMessage={false}
                                     specialType={"TEXTAREA"}
                                     storeKey={storeKey}
                                     validationChecked={false}
                                     validateOnMount={false}
                                     value={typeof searchValue === "string" ? searchValue : null}
                        />
                    }

                </Col>

            </Row>
        </React.Fragment>
    )
};

SearchByObjectIdInner.propTypes = {

    storeKey: PropTypes.string.isRequired,
};

export const SearchByObjectId = memo(SearchByObjectIdInner);