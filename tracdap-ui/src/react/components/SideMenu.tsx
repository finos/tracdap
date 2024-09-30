/**
 * A component that shows moves in and out on the left-hand side when toggled.
 *
 * @module SideMenu
 * @category Component
 */

import {ExpandableMenuWrapper} from "./ExpandableMenuWrapper";
import Image from "react-bootstrap/Image";
import React from "react";
import {useAppSelector} from "../../types/types_hooks";

export const SideMenu = () => {

    console.log("Rendering SideMenu")

    // Get what we need from the store
    const {show} = useAppSelector(state => state["applicationStore"].sideMenu)
    const {"trac-theme": theme} = useAppSelector(state => state["applicationStore"].cookies)
    const {application} = useAppSelector(state => state["applicationStore"].clientConfig.images)

    return (

        <div id="side-menu" className={`${show ? "show" : "hide"}`}>

            <Image className={"mx-auto d-block mt-5"}
                   width={application.lightBackground.displayWidth * 50 / application.lightBackground.displayHeight}
                   height={50}
                   src={application.lightBackground.src}
                   alt={application.lightBackground.alt}
            />

            <div className={"mt-3 mx-4"}>

                {/*//@ts-ignore*/}
                <ExpandableMenuWrapper theme={theme}/>

            </div>
        </div>
    )
};