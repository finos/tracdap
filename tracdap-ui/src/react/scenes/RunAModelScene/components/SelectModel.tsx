/**
 * A component that allows the user to select a model to run based on searching using the business
 * segments that they have selected.
 * @module SelectModel
 * @category RunAModelScene component
 */

import {addModelOption, buildRunAModelJobConfiguration, getModels, setModel, updateOptionLabels} from "../store/runAModelStore";
import Col from "react-bootstrap/Col";
import Confirm from "../../../components/Confirm";
import {ModelInfoModal} from "../../../components/ModelInfoModal";
import {HeaderTitle} from "../../../components/HeaderTitle";
import React, {useCallback, useEffect, useState} from "react";
import Row from "react-bootstrap/Row"
import {SelectOption} from "../../../components/SelectOption";
import {TextBlock} from "../../../components/TextBlock";
import {Toolbar} from "./Toolbar";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const SelectModel = () => {

    // Get what we need from the stores, note the multiple destructuring is done to prevent
    // re-renders when other parts of the UI update
    const {status: modelsStatus, modelOptions, selectedModelOption} = useAppSelector(state => state["runAModelStore"].models)
    const {status: modelStatus, jobHasBeenBuilt} = useAppSelector(state => state["runAModelStore"].model)
    const {userChanged} = useAppSelector(state => state["runAModelStore"].model)
    const {showUpdatedDate, showCreatedDate, showObjectId, showVersions} = useAppSelector(state => state["runAModelStore"].optionLabels.models)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Whether to show the modal containing info about the model.
    const [show, setShow] = useState<boolean>(false)

    /**
     * A function that toggles showing the info modal for the user selected model.
     */
    const toggleInfoModal = useCallback(() => {
        setShow(show => !show)
    }, [])

    /**
     * A hook that runs when the selected model changes, this builds the job configuration object for the new model.
     * Placing this outside of a reducer means that we can update the page fom external calls to change the option.
     * For example if the user wants to re-run a job then all we have to do is set the option to that model. Since the
     * user can go back and forth between setting and reviewing the job set up we need to also check that the model has
     * not changed.
     */
    useEffect(() => {

        // This function to build the job configuration needs to run when the selected model changes, however when the
        // user reviews their job they might come back to the setup stage. Under this circumstance we do not want to
        // trigger this function. Also, we want to rebuild the model when the user requests to rerun a job.
        if (selectedModelOption && !jobHasBeenBuilt) {
            dispatch(buildRunAModelJobConfiguration())
        }

    }, [dispatch, jobHasBeenBuilt, selectedModelOption])

    return (

        <React.Fragment>

            <HeaderTitle text={"Select a model"}
                         type={"h3"}
            />

            <TextBlock>
                The list below shows all of the different models that can be run for your selected categories.
                This can include both models that are in governance and challenger models. If you know the
                object ID of the model you want to run then paste that into the select and click enter.
                 Note that no overlays can be applied to the model inputs and outputs.
            </TextBlock>

            <Row className={"mt-2 pb-4"}>
                <Col xs={10} md={9} lg={8} xl={8}>

                    {/*Note that when wrapped in a Confirm  component the SelectOption is set to not dispatch the onChange*/}
                    {/*action but this responsibility is passed onto the Confirm component*/}

                    {/*Do not show a confirmation message if no model has been loaded or the one loaded */}
                    {/*has had no changes made to it*/}
                    <Confirm isDispatched={true}
                             description={"If you change the model you want to run then your changes to the job set up will be lost. Please confirm that you want to continue."}
                             ignore={Boolean(!(modelStatus === "succeeded" && userChanged.something))}
                    >{confirm => (
                        <SelectOption basicType={trac.STRING}
                                      hideDisabledOptions={true}
                                      onChange={confirm(setModel)}
                                      options={modelOptions}
                                      isDispatched={false}
                                      isLoading={Boolean(modelsStatus === "pending" || modelStatus === "pending")}
                                      objectType={trac.ObjectType.MODEL}
                                      showValidationMessage={false}
                                      mustValidate={false}
                                      validateOnMount={false}
                                      hideDropdown={false}
                                      isCreatable={true}
                                      value={selectedModelOption}
                                      onCreateNewOption={addModelOption}
                        />
                    )}
                    </Confirm>

                </Col>

                <Col xs={2} md={3} lg={4} xl={4} className={"my-auto"}>

                    <Toolbar name={"models"}
                             storeKey={"runAModel"}
                             onRefresh={getModels}
                             onShowInfo={selectedModelOption ? toggleInfoModal : undefined}
                             onChangeLabel={updateOptionLabels}
                             showObjectId={showObjectId}
                             showVersions={showVersions}
                             showCreatedDate={showCreatedDate}
                             showUpdatedDate={showUpdatedDate}
                             disabled={Boolean(modelsStatus === "pending" || modelStatus === "pending")}
                    />
                </Col>
            </Row>

            {selectedModelOption &&
                <ModelInfoModal show={show}
                                tagSelector={selectedModelOption.tag.header}
                                toggle={toggleInfoModal}
                />
            }

        </React.Fragment>
    )
};