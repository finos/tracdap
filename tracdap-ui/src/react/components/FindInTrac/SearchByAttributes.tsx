/**
 * A component that shows a menu that allows the user to select what attributes they want to search by in TRAC.
 *
 * @module SearchByAttributes
 * @category Component
 */

import {
    type FindInTracStoreState,
    setAttributeObjectTypeFilter,
    setBusinessSegment,
    setCreateOrUpdate,
    setDateRangeFilter,
    setDayOrMonth,
    setJobFilter,
    setModelFilter,
    setShowInSearchResultsFilter,
    setUserFilter
} from "./findInTracStore";
import Col from "react-bootstrap/Col";
import {convertBusinessSegmentDataToOptions, getModelRepositories} from "../../utils/utils_general";
import {DoSearchButton} from "./DoSearchButton";
import {HeaderTitle} from "../HeaderTitle";
import PropTypes from "prop-types";
import React, {memo, useMemo} from "react";
import Row from "react-bootstrap/Row";
import SelectDateRange from "../SelectDateRange";
import {SelectOption} from "../SelectOption";
import {SelectUser} from "../SelectUser";
import {SelectValue} from "../SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the SearchByAttributes component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

const SearchByAttributesInner = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {
        filtersToShow,
        general: {
            dateRange: {startDate, endDate, dayOrMonth, createOrUpdate: createOrUpdateDateRange},
            objectType,
            businessSegments,
            user: {createOrUpdate: createOrUpdateUser, selectedUserOption},
            showInSearchResults
        },
        job: {jobType, jobStatus},
        model: {modelPath, modelRepository}
    } = useAppSelector(state => state["findInTracStore"].uses[storeKey].filters)

    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)

    const {userOptions} = useAppSelector(state => state["applicationStore"])

    const {codeRepositories} = useAppSelector(state => state["applicationStore"].clientConfig)

    const {tooltips} = useAppSelector(state => state["findInTracStore"].resultsSettings)

    const {data, fields} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_business_segment_options.currentDefinition)

    // Create a set of options for each of the model repositories defined in the config, the models loaded into TRAC
    // will be tagged with one of these repositories
    const modelRepositoryOptions = [{
        value: "ALL",
        label: "All"
    }, ...getModelRepositories(codeRepositories, tenant).map(repository => ({
        value: repository.tracConfigName,
        label: `${repository.name} (${repository.tracConfigName})`
    }))]

    // If the dataset changes this set of options should update
    const businessSegmentOptions = useMemo(() => convertBusinessSegmentDataToOptions(data, fields, true), [data, fields])

    // The options for the show_in_search_results attribute
    const showInSearchResultsOptions = [{value: null, label: "All"}, {value: true, label: "True"}, {value: false, label: "False"}]

    const showJobFilters = objectType.type === trac.ObjectType.JOB && (filtersToShow.includes("jobType") || filtersToShow.includes("jobStatus"))

    const showModelFilters = objectType.type === trac.ObjectType.MODEL && (filtersToShow.includes("modelRepository") || filtersToShow.includes("modelPath"))

    return (
        <React.Fragment>

            {/*It doesn't make sense to have a header if there is only one section*/}
            {(showJobFilters || showModelFilters) &&
                <HeaderTitle type={'h5'} text={"Main filters"}/>
            }

            <Row className={ (showJobFilters || showModelFilters)? "" : "mt-3"}>
                {filtersToShow.includes("objectType") &&
                    <Col xs={12} md={6} lg={4} xl={3} className={"mb-4"}>

                        <SelectOption basicType={trac.STRING}
                                      labelText={"Object type:"}
                                      mustValidate={false}
                                      name={"objectType"}
                                      onChange={setAttributeObjectTypeFilter}
                                      options={Types.tracObjectTypes}
                                      showValidationMessage={false}
                                      storeKey={storeKey}
                                      tooltip={tooltips.objectType}
                                      validateOnMount={false}
                                      value={objectType}
                        />
                    </Col>
                }

                {filtersToShow.includes("updatedDate") &&
                    <Col xs={12} md={12} lg={9} xl={9} className={"mb-4"}>

                        <SelectDateRange createOrUpdate={createOrUpdateDateRange}
                                         dayOrMonth={dayOrMonth}
                                         endDate={endDate}
                                         isDispatched={true}
                                         labelText={"Date:"}
                                         name={"dateRange"}
                                         onChange={setDateRangeFilter}
                                         setDayOrMonth={setDayOrMonth}
                                         setCreateOrUpdate={setCreateOrUpdate}
                                         startDate={startDate}
                                         storeKey={storeKey}
                                         tooltip={tooltips.dateRange}
                        />
                    </Col>
                }

                {filtersToShow.includes("showInSearchResults") &&
                    <Col xs={12} md={12} lg={6} xl={3} className={"mb-4"}>

                        <SelectOption basicType={trac.BOOLEAN}
                                      labelText={"Shown in searches:"}
                                      mustValidate={false}
                                      name={"showInSearchResults"}
                                      onChange={setShowInSearchResultsFilter}
                                      options={showInSearchResultsOptions}
                                      showValidationMessage={false}
                                      storeKey={storeKey}
                                      tooltip={tooltips.showInSearchResults}
                                      validateOnMount={false}
                                      value={showInSearchResults}
                        />
                    </Col>
                }

                {filtersToShow.includes("businessSegments") &&
                    <Col xs={12} md={12} lg={6} xl={6} className={"mb-4"}>

                        <SelectOption basicType={trac.STRING}
                                      isClearable={true}
                                      isMulti={true}
                                      labelText={"Business segments:"}
                                      mustValidate={false}
                                      name={"businessSegments"}
                                      onChange={setBusinessSegment}
                                      options={businessSegmentOptions}
                                      storeKey={storeKey}
                                      showValidationMessage={false}
                                      tooltip={tooltips.businessSegments}
                                      validateOnMount={false}
                                      value={businessSegments}
                        />
                    </Col>
                }

                {filtersToShow.includes("user") &&
                    <Col xs={12} md={12} lg={6} xl={6} className={"mb-4"}>

                        <SelectUser createOrUpdate={createOrUpdateUser}
                                    labelText={"User:"}
                                    name={"user"}
                                    onChange={setUserFilter}
                                    options={[{value: "ALL", label: "All"}, ...userOptions]}
                                    storeKey={storeKey}
                                    setCreateOrUpdate={setCreateOrUpdate}
                                    tooltip={tooltips.user}
                                    value={selectedUserOption}
                        />
                    </Col>
                }
            </Row>

            {showJobFilters &&
                <React.Fragment>
                    <HeaderTitle type={'h5'} text={"Job filters"}/>

                    <Row>

                        {filtersToShow.includes("jobType") &&

                            <Col xs={12} md={6} lg={4} xl={3} className={"mb-4"}>
                                <SelectOption basicType={trac.STRING}
                                              labelText={"Job type:"}
                                              mustValidate={false}
                                              name={"jobType"}
                                              onChange={setJobFilter}
                                              options={Types.tracJobTypes}
                                              showValidationMessage={false}
                                              storeKey={storeKey}
                                              tooltip={tooltips.jobType}
                                              validateOnMount={false}
                                              value={jobType}
                                />
                            </Col>
                        }

                        {filtersToShow.includes("jobStatus") &&
                            <Col xs={12} md={6} lg={4} xl={3} className={"mb-4"}>
                                <SelectOption basicType={trac.STRING}
                                              labelText={"Job status:"}
                                              mustValidate={false}
                                              name={"jobStatus"}
                                              onChange={setJobFilter}
                                              options={Types.tracJobStatuses}
                                              showValidationMessage={false}
                                              storeKey={storeKey}
                                              tooltip={tooltips.jobType}
                                              validateOnMount={false}
                                              value={jobStatus}
                                />
                            </Col>
                        }
                    </Row>
                </React.Fragment>
            }

            {showModelFilters &&
                <React.Fragment>
                    <HeaderTitle type={'h5'} text={"Model filters"}/>

                    <Row>

                        {filtersToShow.includes("modelRepository") &&
                            <Col xs={12} md={6} lg={4} xl={3} className={"mb-4"}>

                                <SelectOption basicType={trac.STRING}
                                              labelText={"Model repository:"}
                                              mustValidate={false}
                                              name={"modelRepository"}
                                              onChange={setModelFilter}
                                              options={modelRepositoryOptions}
                                              showValidationMessage={false}
                                              storeKey={storeKey}
                                              tooltip={tooltips.jobType}
                                              validateOnMount={false}
                                              value={modelRepository}
                                />
                            </Col>
                        }
                        {filtersToShow.includes("modelPath") &&

                            <Col xs={12} md={6} lg={4} xl={3} className={"mb-4"}>
                                <SelectValue basicType={trac.STRING}
                                             labelText={"Model path:"}
                                             mustValidate={false}
                                             name={"modelPath"}
                                             onChange={setModelFilter}
                                             placeHolderText={"All"}
                                             showValidationMessage={false}
                                             storeKey={storeKey}
                                             tooltip={tooltips.modelPath}
                                             validateOnMount={false}
                                             value={modelPath}
                                />
                            </Col>
                        }

                    </Row>
                </React.Fragment>
            }
            <Row>
                <Col xs={12} className={"mb-2"}>
                    <DoSearchButton storeKey={storeKey}/>
                </Col>
            </Row>

        </React.Fragment>
    )
};

SearchByAttributesInner.propTypes = {

    storeKey: PropTypes.string.isRequired,
};

export const SearchByAttributes = memo(SearchByAttributesInner);