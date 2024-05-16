/**
 * A component that shows a menu bar at the top of the user interface.
 *
 * @module TopMenu
 * @category Component
 */

import {AboutModal} from "./AboutModal";
import {Burger} from "./Burger";
import {Button} from "./Button";
import Container from "react-bootstrap/Container";
import {Icon} from "./Icon"
import React, {useCallback, useState} from "react";
import {SettingsModal} from "./SettingsModal";
import {toggleSideMenu} from "../store/applicationStore";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";

export const TopMenu = () => {

    console.log("Rendering TopMenu")

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {userName} = useAppSelector(state => state["applicationStore"].login)
    const {environment} = useAppSelector(state => state["applicationStore"].platformInfo)
    const {show} = useAppSelector(state => state["applicationStore"].sideMenu)

    const {
        "trac-theme": theme,
        "trac-tenant": tenant,
        "trac-language": language
    } = useAppSelector(state => state["applicationStore"].cookies)

    const {
        searchAsOf
    } = useAppSelector(state => state["applicationStore"].tracApi)

    /**
     * A hook for whether the settings menu is shown.
     */
    const [showSettingsModal, setShowSettingsModal] = useState<boolean>(false)

    /**
     * A hook for whether the about menu is shown.
     */
    const [showAboutModal, setShowAboutModal] = useState<boolean>(false)

    /**
     * A wrapper function that changes whether the settings modal
     * is shown or not.
     */
    const toggleSettingsModal = useCallback(() => {
        setShowSettingsModal(showSettingsModal => !showSettingsModal)
    }, [])

    /**
     * A wrapper function that changes whether the about modal
     * is shown or not.
     */
    const toggleAboutModal = useCallback(() => {
        setShowAboutModal(showSettingsModal => !showSettingsModal)
    }, [])

    return (

        <div id="top-menu">
            <Container className={"my-auto"}>
                <div className={"d-flex justify-content-between"}>

                    <Burger ariaLabel={"Open and close main menu"}
                            open={show}
                            size={"md"}
                            onClick={() => dispatch(toggleSideMenu())}
                    />

                    <div className={"d-flex align-items-center"}>
                        <div className={"fs-13"}>
                            <div>Logged in as {userName}</div>
                            <div>Running from {environment || "unknown"}</div>
                        </div>
                        <Button ariaLabel={"Open and close settings menu"}
                                className={"ms-3 m-0 p-0 position-relative border-0"}
                                isDispatched={false}
                                onClick={toggleSettingsModal}
                                variant={"link"}
                        >
                            <Icon ariaLabel={false}
                                  className={"text-secondary"}
                                  icon={"bi-gear"}
                                  size={"1.5rem"}
                            />
                            {/*This is a small badge that denotes an issue with the settings modal*/}
                            {(!theme || !tenant || !language || searchAsOf) ?
                                <span
                                    className="position-absolute top-0 start-100 translate-middle mt-2 mr-2 p-2 bg-danger border rounded-circle">
                                <span className="visually-hidden">New alerts</span>
                            </span> : null
                            }
                        </Button>

                        <Button ariaLabel={"Open and close about menu"}
                                className={"ms-3 m-0 p-0 position-relative border-0"}
                                isDispatched={false}
                                onClick={toggleAboutModal}
                                variant={"link"}
                        >
                            <Icon ariaLabel={false}
                                  className={"text-secondary"}
                                  icon={"bi-info-circle"}
                                  size={"1.5rem"}
                            />
                        </Button>

                    </div>
                </div>
            </Container>

            <SettingsModal show={showSettingsModal}
                           toggle={toggleSettingsModal}
            />

            <AboutModal show={showAboutModal}
                        toggle={toggleAboutModal}
            />
        </div>
    )
};