/**
 * A component that shows a footer on the bottom of the page. If project resources are set in the config it will have
 * links to the pages/repositories such as the model code.
 *
 * @module BottomMenu
 * @category Component
 */

import {A} from "./A";
import Container from "react-bootstrap/Container";
import type {ManagementTools} from "../../types/types_general";
import React from "react";
import {useAppSelector} from "../../types/types_hooks";

import {ReactComponent as BitBucketIcon} from "../../images/bitbucket-brands.svg";
import {ReactComponent as ConfluenceIcon} from "../../images/confluence-brands.svg";
import {ReactComponent as GitHubIcon} from "../../images/github-brands.svg";
import {ReactComponent as GitKrakenIcon} from "../../images/gitkraken-brands.svg";
import {ReactComponent as GitLabIcon} from "../../images/gitlab-brands.svg";
import {ReactComponent as JiraIcon} from "../../images/jira-brands.svg";
import {ReactComponent as NexusIcon} from "../../images/git-alt-brands.svg";

// A lookup object that converts the icon to a component that renders the SVG icon
const iconLookup: Record<ManagementTools, JSX.Element> = {

    bitbucket: <BitBucketIcon/>,
    confluence: <ConfluenceIcon/>,
    github: <GitHubIcon/>,
    gitkraken: <GitKrakenIcon/>,
    gitlab: <GitLabIcon/>,
    jira: <JiraIcon/>,
    nexus: <NexusIcon/>
}

export const BottomMenu = () => {

    // Get what we need from the store
    const {externalLinks} = useAppSelector(state => state["applicationStore"].clientConfig)

    return (

        <div id="bottom-menu">
            <Container>
                <div className={"text-center"}>
                    {Object.entries(externalLinks).map(([key, item]) =>
                        <React.Fragment key={key}>
                            {item.url && item.application &&
                                <div className={"d-inline-block"}>
                                    <A className={"d-block"} href={item.url}>
                                         {iconLookup[item.application]}
                                    </A>
                                    <span className={"d-block"}>{item.label}</span>
                                </div>
                            }
                        </React.Fragment>
                    )}
                </div>
            </Container>
        </div>
    )
};