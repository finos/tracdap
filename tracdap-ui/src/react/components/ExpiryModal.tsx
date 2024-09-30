/**
 * A component that shows a modal that is only visible if the demo period has expired, it prevents the user from using the application.
 * The side menu and some orchestration services are also turned off.
 *
 * @module ExpiryModal
 * @category Component
 */

import {A} from "./A";
import Col from "react-bootstrap/Col";
import Image from "react-bootstrap/Image";
import {getDaysToExpiry, sOrNot} from "../utils/utils_general";
import Modal from "react-bootstrap/Modal";
import React, {useState} from "react";
import Row from "react-bootstrap/Row";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

const daysToExpiry = getDaysToExpiry()

export const ExpiryModal = () => {

    // Give warnings seven days before expiry
    const [show, setShow] = useState(Boolean(daysToExpiry <= 7))

    return (

        <Modal backdrop={"static"}
               keyboard={false}
               onHide={() => setShow(false)}
               show={show}
        >

            {/* Only allow closing the model if the expiry has not passed, and we are showing a warning*/}
            <Modal.Header closeButton={Boolean(daysToExpiry >= 0)}>
                <Modal.Title className={Boolean(daysToExpiry < 0) ? "mx-auto" : undefined}>
                    {Boolean(daysToExpiry >= 0) ? "Your TRAC-UI demo will expire soon" : "Your TRAC-UI demo period has expired"}
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col className={"mt-1 mb-4"} xs={xsGrid} md={mdGrid} lg={lgGrid}>
                        {/*TRAC logo*/}
                        <Image alt={"Accenture"}
                               className={"mx-auto d-block pe-3 mb-3"}
                               height={57}
                               src={"/trac-ui/app/static/images/accenture_logo.png"}
                               width={200}
                        />
                    </Col>

                    <Col className={"mb-5 d-flex justify-content-center"} xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        {daysToExpiry >= 0 &&
                            < div className={"lh-lg text-justify"}>
                            We hope that you are enjoying your TRAC-UI demo. The demo period will end {daysToExpiry > 0 ? "in " + daysToExpiry + " day" + sOrNot(daysToExpiry) : "today"}. If you would like to discuss
                            TRAC-UI further then please contact <A href={`mailto:greg.wiltshire@accenture.com;martin.traverse@accenture.com?subject=${encodeURIComponent("TRAC-UI enquiry")}`}>
                            Accenture</A>. When the demo period expires the installed open source TRAC services will still be available.
                            </div>

                        }

                        {daysToExpiry < 0 &&
                            < div className={"lh-lg text-justify"}>
                            We hope that you enjoyed your experience with TRAC-UI. The demo period has now ended and this application is locked. If you would like to discuss
                            TRAC-UI further then please contact <A href={`mailto:greg.wiltshire@accenture.com;martin.traverse@accenture.com?subject=${encodeURIComponent("TRAC-UI enquiry")}`}>
                            Accenture</A>. The installed open source TRAC services are unaffected by this and are still available.
                            </div>
                        }

                    </Col>
                </Row>
            </Modal.Body>
        </Modal>
    )
};