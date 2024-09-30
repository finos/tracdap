/**
 * A component that allows the user to select a flow to run based on searching using the business
 * segments that they have selected.
 * @module SelectFlow
 * @category RunAFlowScene component
 */

import {addFlowOption, buildRunAFlowJobConfiguration, getFlows, setFlow, updateOptionLabels} from "../store/runAFlowStore";
import Col from "react-bootstrap/Col";
import Confirm from "../../../components/Confirm";
import {FlowInfoModal} from "../../../components/FlowInfoModal";
import {HeaderTitle} from "../../../components/HeaderTitle";
import PropTypes from "prop-types";
import React, {useCallback, useEffect, useState} from "react";
import Row from "react-bootstrap/Row"
import {SelectOption} from "../../../components/SelectOption";
import {TextBlock} from "../../../components/TextBlock";
import {Toolbar} from "./Toolbar";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const SelectFlow = () => {

    // Get what we need from the stores, note the multiple destructuring is done to prevent
    // re-renders when other parts of the UI update
    const {status: flowsStatus, flowOptions, selectedFlowOption} = useAppSelector(state => state["runAFlowStore"].flows)
    const {status: flowStatus, jobHasBeenBuilt, userChanged} = useAppSelector(state => state["runAFlowStore"].flow)
    const policiesStatus = useAppSelector(state => state["runAFlowStore"].policies.status)
    const policyStatus = useAppSelector(state => state["runAFlowStore"].policy.status)
    const {showUpdatedDate, showCreatedDate, showObjectId, showVersions} = useAppSelector(state => state["runAFlowStore"].optionLabels.flows)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Whether to show the modal containing info about the flow.
    const [show, setShow] = useState<boolean>(false)

    /**
     * A function that toggles showing the info modal for the user selected flow.
     */
    const toggleInfoModal = useCallback(() => {
        setShow(show => !show)
    }, [])

    /**
     * A hook that runs when the selected flow changes, this builds the flow object for the new flow. Placing this
     * outside of a reducer means that we can update the page fom external calls to change the option. For example if
     * the user wants to re-run a job then all we have to do is set the option to that flow. Since the user can go
     * back and forth between setting and reviewing the job set up we need to also check that the flow has not
     * changed.
     */
    useEffect(() => {

        // This function to build the flow needs to run when the selected flow changes, however when the user
        // reviews their job they might come back to the setup stage. Under this circumstance we do not want to
        // trigger this function. Also, we want to rebuild the flow when the user requests to rerun a job.
        if (selectedFlowOption && !jobHasBeenBuilt) {
            dispatch(buildRunAFlowJobConfiguration())
        }

    }, [dispatch, jobHasBeenBuilt, selectedFlowOption])

    return (

        <React.Fragment>

            <HeaderTitle text={"Select a flow"}
                         tooltip={"A flow is a group of models run as a single calculation or process"}
                         type={"h3"}
            />

            <TextBlock>
                The list below shows all of the different flows that can be run for your selected categories.
                Flows can include models that are in governance, deprecated or not in use yet. If you know the
                object ID of the flow you want to run then paste the ID into the select and click enter.
            </TextBlock>

            <Row className={"mt-2 pb-4"}>
                <Col xs={10} md={9} lg={8} xl={8}>

                    {/*Note that when wrapped in a Confirm  component the SelectOption is set to not dispatch the onChange*/}
                    {/*action but this responsibility is passed onto the Confirm component*/}

                    {/*Do not show a confirmation message if no flow has been loaded or the one loaded */}
                    {/*has had no changes made to it*/}
                    <Confirm description={"If you change the flow you want to run then your changes to the job set up will be lost. Please confirm that you want to continue."}
                             ignore={Boolean(!(flowStatus === "succeeded" && userChanged.something))}
                             isDispatched={true}
                    >{confirm => (
                        <SelectOption basicType={trac.STRING}
                                      hideDisabledOptions={true}
                                      onChange={confirm(setFlow)}
                                      options={flowOptions}
                                      isDispatched={false}
                                      isLoading={Boolean(flowsStatus === "pending" || flowStatus === "pending" || policiesStatus === "pending" || policyStatus === "pending")}
                                      objectType={trac.ObjectType.FLOW}
                                      showValidationMessage={false}
                                      mustValidate={false}
                                      validateOnMount={false}
                                      hideDropdown={false}
                                      isCreatable={true}
                                      value={selectedFlowOption}
                                      onCreateNewOption={addFlowOption}
                        />
                    )}
                    </Confirm>

                </Col>

                <Col xs={2} md={3} lg={4} xl={4} className={"my-auto"}>

                    <Toolbar name={"flows"}
                             storeKey={"runAFlow"}
                             onRefresh={getFlows}
                             onShowInfo={selectedFlowOption ? toggleInfoModal : undefined}
                             onChangeLabel={updateOptionLabels}
                             showObjectId={showObjectId}
                             showVersions={showVersions}
                             showCreatedDate={showCreatedDate}
                             showUpdatedDate={showUpdatedDate}
                             disabled={Boolean(flowsStatus === "pending" || flowStatus === "pending")}
                    />
                </Col>
            </Row>

            {selectedFlowOption &&
                <FlowInfoModal show={show}
                               storeKey={"runAFlow"}
                               tagSelector={selectedFlowOption.tag.header}
                               toggle={toggleInfoModal}
                />
            }

        </React.Fragment>
    )
};

SelectFlow.prototype = {

    storeKey: PropTypes.string.isRequired
};