/**
 * A component that shows buttons that undo any tag edits or initiates the update in TRAC.
 *
 * @module UpdateTagButtons
 * @category Component
 */

import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {Icon} from "../../../components/Icon";
import {objectsContainsValue} from "../../../utils/utils_object";
import React from "react";
import Row from "react-bootstrap/Row";
import {setAttributesFromTag, setValidationChecked} from "../../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../../utils/utils_general";
import {updateTagInTrac} from "../store/updateTagsStore";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const UpdateTagButtons = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {isValid} = useAppSelector(state => state["setAttributesStore"].uses.updateTags.attributes.validation)
    const {status, tag} = useAppSelector(state => state["updateTagsStore"])

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a tag update into TRAC.
     */
    const handleUpdateTagInTrac = (): void => {

        if (objectsContainsValue(isValid, false)) {

            dispatch(setValidationChecked({storeKey: "updateTags", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            setValidationChecked({storeKey: "updateTags", value: false})
            dispatch(updateTagInTrac())
        }
    }

    return (
        <React.Fragment>
            {/*Show if we have a selected row in the table*/}
            {tag &&

                <React.Fragment>

                    <Row>
                        <Col>
                            <Button ariaLabel={"Save changes"}
                                    className={"min-width-px-150 float-end"}
                                    isDispatched={false}
                                    loading={Boolean(status === "pending")}
                                    onClick={handleUpdateTagInTrac}
                            >
                                <Icon icon={"bi-save"}
                                      ariaLabel={false}
                                      className={"me-2"}
                                />Save changes
                            </Button>

                            <Button ariaLabel={"Undo changes"}
                                    className={"min-width-px-150 float-end me-2"}
                                    isDispatched={false}
                                    onClick={() => dispatch(setAttributesFromTag({tag, storeKey: "updateTags"}))}
                            >
                                <Icon icon={"bi-arrow-counterclockwise"}
                                      ariaLabel={false}
                                      className={"me-2"}
                                />Undo changes
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};