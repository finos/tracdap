/**
 * A component that shows a modal top right in the screen that shows versions
 * of the various services used and any TRAC-UI deployment information.
 *
 * @module AboutModal
 * @category Component
 */

import {Button} from "./Button";
import Col from "react-bootstrap/Col";
import {convertDateObjectToFormatCode} from "../utils/utils_formats";
import {DeploymentInfoTable} from "./DeploymentInfoTable";
import {getExpiry} from "../utils/utils_general";
import {Icon} from "./Icon";
import Image from "react-bootstrap/Image";
import ListGroup from "react-bootstrap/ListGroup";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../types/types_hooks";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

/**
 * An interface for the props of the AboutModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<boolean>>
}

// The date of the license expiry
const expiryDate = getExpiry()

export const AboutModal = (props: Props) => {

    const {show, toggle} = props

    // Get what we need from the store
    const {tracVersion, production, environment} = useAppSelector(state => state["applicationStore"].platformInfo)
    const {application} = useAppSelector(state => state["applicationStore"].clientConfig.images)

    return (

        // dialogClassName adds a class to make the modal appear top right
        <Modal show={show} onHide={() => toggle(false)} dialogClassName="modal-dialog-top-right">

            <Modal.Header closeButton>
                <Modal.Title>
                    About
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col className={"mt-1"} xs={xsGrid} md={mdGrid} lg={lgGrid}>
                        {/*TRAC logo*/}
                        <Image className={"mx-auto d-block mb-3"}
                               width={application.lightBackground.displayWidth * 75 / application.lightBackground.displayHeight}
                               height={75}
                               src={application.lightBackground.src}
                               alt={application.lightBackground.alt}
                        />
                    </Col>

                    <Col className={"my-1 d-flex justify-content-center"} xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        <ListGroup className="list-group-flush">
                            <ListGroup.Item className={"pointer fs-13"}>
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-hexagon"}
                                      size={"0.875rem"}
                                />Running TRAC-UI v{TRAC_UI_VERSION}
                            </ListGroup.Item>
                            <ListGroup.Item className={"pointer fs-13"}>
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-hexagon"}
                                      size={"0.875rem"}
                                />Running TRAC v{tracVersion}
                            </ListGroup.Item>
                            <ListGroup.Item className={"pointer fs-13"}>
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-hexagon"}
                                      size={"0.875rem"}
                                />Environment: {environment}
                            </ListGroup.Item>
                            <ListGroup.Item className={"pointer fs-13"}>
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-hexagon"}
                                      size={"0.875rem"}
                                />{production ? 'Production' : 'Non-production'} version
                            </ListGroup.Item>
                            {expiryDate &&
                                <ListGroup.Item className={"pointer fs-13"}>
                                    <Icon ariaLabel={false}
                                          className={"me-2"}
                                          icon={"bi-hexagon"}
                                          size={"0.875rem"}
                                    />Demo licence expiry: {convertDateObjectToFormatCode(expiryDate)}
                                </ListGroup.Item>
                            }
                            <ListGroup.Item>
                                &copy; {new Date().getFullYear()} Accenture TRAC-UI. All Rights Reserved.
                            </ListGroup.Item>
                        </ListGroup>

                    </Col>
                    <Col className={"mt-1"} xs={xsGrid} md={mdGrid} lg={lgGrid}>
                        <DeploymentInfoTable showTitle={true}/>
                    </Col>
                </Row>
            </Modal.Body>

            <Modal.Footer>

                <Button ariaLabel={"Close about menu"}
                        variant={"secondary"}
                        onClick={toggle}
                        isDispatched={false}
                >
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

AboutModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};