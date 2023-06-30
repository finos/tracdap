/**
 * This scene is the homepage, it allows the user to pick what they want to do from a set of options
 * displayed as cards in a grid.
 * @module HomeScene
 * @category Scene
 */

import Col from "react-bootstrap/Col";
import {Menu} from "../../../config/config_menu";
import {MenuCard} from "./components/MenuCard";
import React from "react";
import Row from "react-bootstrap/Row";
import {SceneTitle} from "../../components/SceneTitle";
import {useNavigate} from "react-router-dom";

export const HomeScene = () => {

    console.log("Rendering HomeScene")

    // A hook from the React Router plugin that allows us to navigate using onClick events, in this case we move to a
    // page that shows information about the selected object from the table
    const navigate = useNavigate()

    /**
     * A function that runs when the user clicks on a card that navigates them to the page.
     * @param event - The onclick event.
     */
    const handleCardClick = (event: React.MouseEvent) => {

        if (event?.currentTarget?.id) {
            navigate(event?.currentTarget?.id)
        }
    }

    return (

        <React.Fragment>

            <SceneTitle text={"Home"}/>

            <Row>
                {Menu.filter(menuItem => !menuItem.hiddenInHomeMenu).map((menuItem, i) =>

                    <Col className={"my-4"}

                        // The menu allows that multiple paths point to the same page, but only one of these can be shown in the menu
                        // this is for the viewer components where they need to have URL parameters
                         key={Array.isArray(menuItem.path) ? menuItem.path?.[0] : menuItem.path}
                         xs={12}
                         md={4}
                         lg={3}
                    >
                        <MenuCard handleCardClick={handleCardClick} menuItem={menuItem} index={i}/>
                    </Col>
                )}
            </Row>

        </React.Fragment>
    )
};