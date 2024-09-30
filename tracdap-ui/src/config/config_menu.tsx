/**
 * A set of config information for setting up the meni in the {@link SideMenu} component and the navigation
 * information for the react-router plugin set in the {@link Scenes} component.
 * @category Config
 * @module ConfigMenu
 */

import {getDaysToExpiry} from "../react/utils/utils_general";
import {HomeScene} from "../react/scenes/HomeScene";
import {MenuItem, ObjectDictionary} from "../types/types_general";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

const ApplicationSetupScene = React.lazy(() => import("../react/scenes/ApplicationSetupScene"));
const BatchImportDataScene = React.lazy(() => import("../react/scenes/BatchImportDataScene"));
const DataAnalyticsScene = React.lazy(() => import("../react/scenes/DataAnalyticsScene"));
const Error404 = React.lazy(() => import("../react/scenes/Error404"));
const ExamplesScene = React.lazy(() => import("../react/scenes/ExamplesScene"));
const FindAJobScene = React.lazy(() => import("../react/scenes/FindAJobScene"));
const UploadADatasetScene = React.lazy(() => import("../react/scenes/UploadADatasetScene"));
const ObjectSummaryScene = React.lazy(() => import("../react/scenes/ObjectSummaryScene"));
const RunAFlowScene = React.lazy(() => import("../react/scenes/RunAFlowScene"));
const RunAModelScene = React.lazy(() => import("../react/scenes/RunAModelScene"));
const SearchScene = React.lazy(() => import("../react/scenes/SearchScene"));
const UploadAFlowScene = React.lazy(() => import("../react/scenes/UploadAFlowScene"));
const UploadAModelScene = React.lazy(() => import("../react/scenes/UploadAModelScene"));
const UploadASchemaScene = React.lazy(() => import("../react/scenes/UploadASchemaScene"));
const UpdateTagsScene = React.lazy(() => import("../react/scenes/UpdateTagsScene"));

const homeMenuItem: MenuItem = {
    path: '/',
    title: 'Home',
    icon: 'bi-house-door',
    description: null,
    showDefaultHeader: true,
    hiddenInSideMenu: false,
    hiddenInHomeMenu: true,
    expandableMenu: false,
    children: [],
    element: Boolean(getDaysToExpiry() < 0) ? <br/> : <HomeScene/>
}

const errorMenuItem: MenuItem = {
    path: '*',
    title: '404',
    icon: null,
    description: null,
    showDefaultHeader: true,
    hiddenInSideMenu: true,
    hiddenInHomeMenu: true,
    expandableMenu: false,
    children: [],
    element: <Error404/>
}

export const applicationMenuItems: MenuItem[] = [
    {
        path: '/search',
        title: 'Search',
        icon: "bi-search",
        description: "Find anything you need that is stored in TRAC and view its full history.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: false,
        children: [],
        element: <SearchScene title={"Search"}/>

    },
    {
        path: '/run-a-flow',
        title: 'Run a flow',
        icon: "bi-diagram-2",
        description: "Select calculation chains you want to run and chose all of the models and inputs to use.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: false,
        children: [],
        element: <RunAFlowScene/>
    },
    {
        path: '/run-a-model',
        title: 'Run a model',
        icon: "bi-code-square",
        description: "Select individual models you want to run and chose all of the inputs to use.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: false,
        children: [],
        element: <RunAModelScene/>
    },
    {
        path: '/find-a-job',
        title: 'Find a job',
        icon: "bi-binoculars",
        description: "Find jobs that have been run or that are in progress.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: false,
        children: [],
        element: <FindAJobScene title={"Find a job"}/>

    },
    // {
    //     path: 'data-analytics',
    //     title: 'Data analytics',
    //     icon: "bi-graph-up-arrow",
    //     description: "Find jobs and datasets and run SQL analytics on them.",
    //     showDefaultHeader: true,
    //     hiddenInSideMenu: false,
    //        hiddenInHomeMenu: false,
    //     expandableMenu: false,
    //     children: [],
    //     element: <DataAnalyticsScene title={"Data analytics"}/>
    // },
    {
        path: '/view',
        title: "Summaries of objects",
        icon: null,
        description: null,
        showDefaultHeader: true,
        hiddenInSideMenu: true,
        hiddenInHomeMenu: true,
        expandableMenu: true,
        openOnLoad: true,
        children: [
            {
                path: ['model-summary', 'model-summary/:objectId', 'model-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Model summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Model summary"} objectTypeAsString={"MODEL"}/>
            },
            {
                path: ['data-summary', 'data-summary/:objectId', 'data-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Dataset summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Dataset summary"} objectTypeAsString={"DATA"}/>
            },
            {
                path: ['schema-summary', 'schema-summary/:objectId', 'schema-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Schema summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Schema summary"} objectTypeAsString={"SCHEMA"}/>
            },
            {
                path: ['object-summary', 'object-summary/:objectId', 'object-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Object summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Object summary"} objectTypeAsString={"OBJECT_TYPE_NOT_SET"}/>
            },
            {
                path: ['flow-summary', 'flow-summary/:objectId', 'flow-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Flow summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Flow summary"} objectTypeAsString={"FLOW"}/>
            },
            {
                path: ['job-summary', 'job-summary/:objectId', 'job-summary/:objectId/:objectVersion/:tagVersion'],
                title: 'Job summary',
                icon: "bi-dash",
                description: null,
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: true,
                expandableMenu: false,
                children: [],
                element: <ObjectSummaryScene title={"Job summary"} objectTypeAsString={"JOB"}/>
            }
        ],
        element: undefined
    },
    {
        path: '/load',
        title: "Loading tools",
        icon: "bi-minecart-loaded",
        description: "A set of tools for loading items into TRAC.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: true,
        openOnLoad: false,
        children: [
            {
                path: 'upload-a-dataset',
                title: 'Upload a dataset',
                icon: "bi-dash",
                description: 'A tool for uploading csv and excel files as datasets into TRAC.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <UploadADatasetScene title={"Load a dataset"}/>
            },
            {
                path: 'upload-a-schema',
                title: 'Upload a schema',
                icon: "bi-dash",
                description: 'A tool for uploading schemas into TRAC from a code repository.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <UploadASchemaScene title={"Upload a schema"}/>
            },
            {
                path: 'upload-a-model',
                title: 'Upload a model',
                icon: "bi-dash",
                description: 'A tool for uploading models into TRAC from a code repository.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <UploadAModelScene title={"Upload a model"}/>
            },
            {
                path: 'upload-a-flow',
                title: 'Upload a flow',
                icon: "bi-dash",
                description: 'A tool for building and uploading flows into TRAC.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <UploadAFlowScene title={"Upload a flow"}/>
            }
        ],
        element: undefined
    },
    {
        path: '/admin',
        title: "Admin tools",
        icon: "bi-shield-check",
        description: "A set of tools for setting up the application and editing the information stored in TRAC.",
        showDefaultHeader: true,
        hiddenInSideMenu: false,
        hiddenInHomeMenu: false,
        expandableMenu: true,
        openOnLoad: false,
        children: [
            {
                path: 'setup',
                title: 'Set up application',
                icon: "bi-dash",
                description: 'An admin tool for setting up the application in TRAC.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <ApplicationSetupScene title={"Set up application"}/>

            },
            {
                path: 'batch-import-data',
                title: 'Batch import data',
                icon: "bi-dash",
                description: 'An admin tool for saving data moved into the landing area into TRAC.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <BatchImportDataScene title={"Batch import data"}/>
            },
            {
                path: 'update-tags',
                title: 'Update tags',
                icon: "bi-dash",
                description: 'An tool for changing the metadata of an object in TRAC.',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <UpdateTagsScene title={"Update Tags"}/>
            }
        ],
        element: undefined
    },
]

// Only add the UI scenes if the application license has not expired
export const Menu: MenuItem[] = [homeMenuItem, ...(Boolean(getDaysToExpiry() < 0) ? [] : applicationMenuItems), errorMenuItem]

/**
 * If we are not in production we add another scene to show tables, charts and other widgets.
 */
if (process.env.NODE_ENV !== 'production') {
    Menu.push({
        path: '/example',
        title: "Examples",
        icon: "bi-lightbulb",
        description: "A set of widgets useful for when giving demos.",
        showDefaultHeader: true,
        hiddenInSideMenu: true,
        hiddenInHomeMenu: false,
        expandableMenu: true,
        openOnLoad: false,
        children: [
            {
                path: 'setup',
                title: 'Example widgets',
                icon: "bi-dash",
                description: 'An set of examples of the charts, tables and widgets available in TRAC-ui',
                showDefaultHeader: true,
                hiddenInSideMenu: false,
                hiddenInHomeMenu: false,
                expandableMenu: false,
                children: [],
                element: <ExamplesScene title={"Example widgets"}/>

            }
        ],
        element: undefined
    })
}

// A lookup to where to navigate to based on the type of object that the user has clicked on.
const ObjectSummaryPaths: ObjectDictionary = {
    [trac.ObjectType.OBJECT_TYPE_NOT_SET]: {to: "/view/object-summary"},
    [trac.ObjectType.MODEL]: {to: "/view/model-summary"},
    [trac.ObjectType.JOB]: {to: "/view/job-summary"},
    [trac.ObjectType.DATA]: {to: "/view/data-summary"},
    [trac.ObjectType.FLOW]: {to: "/view/flow-summary"},
    [trac.ObjectType.SCHEMA]: {to: "/view/schema-summary"},
    [trac.ObjectType.CUSTOM]: {to: "/view/custom-summary"},
    [trac.ObjectType.FILE]: {to: "/view/file-summary"},
    [trac.ObjectType.STORAGE]: {to: "/view/storage-summary"}
}

export {ObjectSummaryPaths}