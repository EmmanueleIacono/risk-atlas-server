use std::collections::HashSet;
use serde_json::Value;

use sqlx::{Pool, Postgres, Row};

// HELPERS
pub async fn check_project_description(
    pool: &Pool<Postgres>,
) -> Result<bool, sqlx::Error> {
    let query = r#"
        SELECT COUNT(*) AS proj_descr_exists
        FROM information_schema.columns
        WHERE table_schema = 'tilesets'
        AND table_name = 'project_data'
        AND column_name = 'project_description'
    "#;

    let row = sqlx::query(query)
        .fetch_one(pool)
        .await?;

    let col_exists: i64 = row.try_get("proj_descr_exists")?;

    Ok(col_exists > 0)
}

// modifies the passed node in place
// recurses through all node's children
// returns "true" if the node should be kept, "false" otherwise
pub fn filter_tileset(
    node: &mut Value, // "node" must have "children" -> therefore, at least "root"; will not work with whole "tileset" json
    allowed_classes: &HashSet<String>,
) -> bool {
    /*
    conditions:
        - (1): the current node has a "content"."uri" property      -> has_content
        - (2): the current node has a non-empty "children" property -> has_children
    scenarios:
        - (A): (1) = true,  (2) = false    => if class is allowed, keep
        - (B): (1) = true,  (2) = true     => if class is allowed, keep parent, and filter children individually
        - (C): (1) = false, (2) = true     => if class is allowed, keep parent, and skip filtering children
                                              else, if disallowed, remove parent content, but still recurse children
        - (D): (1) = false, (2) = false    => never keep (unlikely, but not impossible)
    */

    let node_obj = match node.as_object_mut() {
        Some(obj) => obj,
        // None => return true, // if it's not an object, keep it as-is
        None => {
            return true;
        }
    };

    /* ----- READ-ONLY EXTRACTIONS (content?, children?, class?) ----- */

    // Checking condition (1)
    let has_content = node_obj
        .get("content")
        .and_then(|c| c.as_object())
        .and_then(|co| co.get("uri"))
        .is_some();

    // Extracting the class name (and clone to owned String)
    let class_name_opt: Option<String> = node_obj
        .get("metadata")
        .and_then(|mtd| mtd.as_object())
        .and_then(|mtd_obj| mtd_obj.get("class"))
        .and_then(|class_val| class_val.as_str().map(ToOwned::to_owned));

    // Checking condition (2)
    let has_children = {
        let maybe_children = node_obj
            .get("children")
            .and_then(|v| v.as_array());
        maybe_children.map_or(false, |arr| !arr.is_empty())
    };

    // Checking if allowed class
    let is_allowed_class = match class_name_opt {
        Some(ref cls) => allowed_classes.contains(cls),
        None => false, // no class_name => treat as disallowed
    };

    /* ----- FILTERING LOGIC ----- */

    // (D)
    if !has_content && !has_children {
        return false;
    }

    if is_allowed_class {
        // (A) => keep
        // (B) => recurse children
        // (C) => keep children

        // (B)
        if has_content && has_children {
            if let Some(child_val) = node_obj.get_mut("children") {
                if let Some(child_array) = child_val.as_array_mut() {
                    child_array.retain_mut(|child_node| filter_tileset(child_node, allowed_classes));
                }
            }
            // after filtering the children, keep the parent
            return true;

        // (C)
        } else if !has_content && has_children {
            // skip filtering children, keep all (no recursion needed)
            return true;

        // (A)
        } else if has_content && !has_children {
            // leaf node with content, keep
            return true;

        // keep if it reaches here
        } else {
            return true;
        }

    } else { // class is disallowed
        // starting by removing the content (if present)
        node_obj.remove("content");

        // if parent has children, filtering them recursively
        if has_children {
            if let Some(child_val) = node_obj.get_mut("children") {
                if let Some(child_array) = child_val.as_array_mut() {
                    child_array.retain_mut(|child_node| filter_tileset(child_node, allowed_classes));
                }
            }

            // after recursion, if no children remain, remove the current node
            let still_has_children = node_obj
                .get("children")
                .and_then(|v| v.as_array())
                .map_or(false, |arr| !arr.is_empty());

            // now no content -> checking if children
            if still_has_children {
                return true; // keep node as grouping container
            } else {
                return false; // no child left, empty disallowed container
            }

        // no children, no content
        } else {
            return false; // remove
        }
    }
}
