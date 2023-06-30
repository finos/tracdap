/**
 * A component that parses the menu array defined in the config. It effectively returns the scene to show if a match
 * is found in the menu. See {@link https://reactrouter.com/docs/en/v6/api#useroutes| this guide}.
 *
 * @module Scenes
 * @category Component
 */

import cloneDeep from "lodash.clonedeep";
import {MappedMenuItem} from "../../types/types_general";
import {Menu} from "../../config/config_menu";
import React from "react";
import {useRoutes} from "react-router-dom";

// The Menu item needs to be unwound. The react-router plugin does not allow paths to be arrays
// of strings, but we do in the Menu. The Menu has these because we have multiple paths that
// point to the same page (normally the Viewer items that use URL parameters)
const unwrappedMenus: MappedMenuItem[] = cloneDeep(Menu).map(unwrappedMenu => {

    let newChildren: MappedMenuItem[] = []

    unwrappedMenu.children.forEach(child => {

        if (!Array.isArray(child.path)) {
            //@ts-ignore
            newChildren.push(child)
        } else {
            //@ts-ignore
            const unwoundChildren: MappedMenuItem[] = child.path.map(path => ({...child, path: path}))
            newChildren = newChildren.concat(unwoundChildren)
        }
    })

    unwrappedMenu.children = newChildren

    return unwrappedMenu as MappedMenuItem
})

export const Scenes = () => useRoutes(unwrappedMenus)