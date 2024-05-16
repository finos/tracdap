/**
 * A component that allows the user to select a policy to apply to a flow they are trying to run.
 * @module SelectPolicy
 * @category RunAFlowScene component
 */

import Col from "react-bootstrap/Col";
import Confirm from "../../../components/Confirm";
import {getPolicies, getPolicy, updateOptionLabels, setPolicyOption} from "../store/runAFlowStore";
import {HeaderTitle} from "../../../components/HeaderTitle";
import React, {useEffect} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../../components/SelectOption";
import {TextBlock} from "../../../components/TextBlock";
import {Toolbar} from "./Toolbar";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const SelectPolicy = () => {

    // Get what we need from the stores, note the multiple destructuring is done to prevent
    // re-renders when other parts of the UI update
    const {status: flowsStatus} = useAppSelector(state => state["runAFlowStore"].flows)
    const {status: flowStatus, userChanged} = useAppSelector(state => state["runAFlowStore"].flow)
    const {status: policiesStatus, options, selectedPolicyOption} = useAppSelector(state => state["runAFlowStore"].policies)
    const policyStatus = useAppSelector(state => state["runAFlowStore"].policy.status)
    const {showUpdatedDate, showCreatedDate, showObjectId, showVersions} = useAppSelector(state => state["runAFlowStore"].optionLabels.policies)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook that runs when the selected policy changes, this gets the policies for the selected flow. Placing this
     * outside of a reducer means that we can update the page fom external calls to change the option. For example if
     * the user wants to re-run a job then all we have to do is set the option to that flow.
     */
    useEffect(() => {

        if (selectedPolicyOption) dispatch(getPolicy())

    }, [dispatch, selectedPolicyOption])

    return (

        <React.Fragment>
            {flowStatus !== "idle" &&

                <React.Fragment>
                    <HeaderTitle type={"h3"}
                                 text={"Select a policy"}
                                 tooltip={"A policy is set of signed off or in governance models, inputs and parameters for use in a particular flow. They help you know what you should be running."}
                    />

                    <TextBlock>
                        The list below shows all of the different policies that can be applied to your flow. The policy
                        details what signed off models, inputs and parameters should be used. When you select a policy
                        the in policy items will be selected automatically. Signed off items have a green tick next to
                        them.
                    </TextBlock>

                    <Row className={"mt-2 pb-4"}>
                        <Col xs={10} md={9} lg={8} xl={8}>

                            {/*Note that when wrapped in a Confirm component the SelectOption is set to not dispatch the onChange*/}
                            {/*action but this responsibility is passed onto the Confirm component*/}

                            {/*Do not show a confirmation message if no flow has been loaded or the one loaded */}
                            {/*has had no changes made to it*/}
                            <Confirm isDispatched={true}
                                     description={"If you change the policy you want to use then your changes to the job set up will be overwritten to be in line with the new policy. Please confirm that you want to continue."}
                                     ignore={Boolean(!(flowStatus === "succeeded" && userChanged.something))}
                            >{confirm => (

                                <SelectOption basicType={trac.STRING}
                                              onChange={confirm(setPolicyOption)}
                                              options={options}
                                              isDispatched={false}
                                              isLoading={Boolean(policiesStatus === "pending" || policyStatus === "pending") || flowsStatus === "pending" || flowStatus === "pending"}
                                              showValidationMessage={false}
                                              mustValidate={false}
                                              validateOnMount={false}
                                              value={selectedPolicyOption}
                                />
                            )}
                            </Confirm>
                        </Col>

                        <Col xs={2} md={3} lg={4} xl={4} className={"my-auto"}>

                            <Toolbar name={"policies"}
                                     storeKey={"runAFlow"}
                                     onRefresh={getPolicies}
                                     onShowInfo={() => {
                                     }}
                                     onChangeLabel={updateOptionLabels}
                                     showObjectId={showObjectId}
                                     showVersions={showVersions}
                                     showCreatedDate={showCreatedDate}
                                     showUpdatedDate={showUpdatedDate}
                                     disabled={Boolean(policiesStatus === "pending" || flowStatus=== "pending")}
                            />
                        </Col>
                    </Row>

                    {/*TODO Add policy viewing modal*/}

                </React.Fragment>
            }
        </React.Fragment>
    )
};