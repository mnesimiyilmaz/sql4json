package io.github.mnesimiyilmaz.sql4json.utils;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.Getter;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mnesimiyilmaz
 */
public final class JsonUtils {

    private static final ObjectMapper                                  OBJECT_MAPPER;
    private static final CollectionType                                LIST_OF_MAP_TYPE;
    private static final Map<JsonNodeType, Function<JsonNode, Object>> NODE_CONVERSION_MAP;
    private static final Pattern                                       SQUARE_BRACKET_PATTERN = Pattern.compile("\\[\\d+]");

    private JsonUtils() {}


    public static JsonNode peelJsonNode(JsonNode node, String peelingPath) {
        if (peelingPath == null || peelingPath.isEmpty()) {
            return node;
        }
        String[] path = peelingPath.split("\\.");
        for (String p : path) {
            Matcher matcher = SQUARE_BRACKET_PATTERN.matcher(p);
            if (matcher.find()) {
                node = node.get(Integer.parseInt(matcher.group(0).substring(1, matcher.group(0).length() - 1)));
            } else {
                node = node.get(p);
            }
        }
        return node;
    }

    public static List<Map<FieldKey, Object>> convertJsonToFlattenedListOfKeyValue(JsonNode node) {
        if (node.isArray()) {
            List<Map<FieldKey, Object>> returnList = new ArrayList<>();
            node.iterator().forEachRemaining(n -> returnList.add(flattenJsonNode(n)));
            return returnList;
        } else if (node.isObject()) {
            List<Map<FieldKey, Object>> list = new ArrayList<>();
            list.add(flattenJsonNode(node));
            return list;
        } else {
            throw new IllegalArgumentException("Json node must be object or array.");
        }
    }

    private static Map<FieldKey, Object> flattenJsonNode(JsonNode jsonNode) {
        Map<FieldKey, Object> flattenedMap = new HashMap<>();
        flattenJsonNode("", jsonNode, flattenedMap);
        return flattenedMap;
    }

    private static void flattenJsonNode(String currentPath, JsonNode jsonNode, Map<FieldKey, Object> flattenedMap) {
        if (jsonNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();

                String newPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
                flattenJsonNode(newPath, fieldValue, flattenedMap);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                flattenJsonNode(currentPath + "[" + i + "]", arrayNode.get(i), flattenedMap);
            }
        } else {
            String family = SQUARE_BRACKET_PATTERN.matcher(currentPath).replaceAll("");
            flattenedMap.put(FieldKey.of(currentPath, family), getValueAsProperType(jsonNode));
        }
    }

    public static Object getValueAsProperType(JsonNode node) {
        Function<JsonNode, Object> conversionFunction = NODE_CONVERSION_MAP.get(node.getNodeType());
        if (conversionFunction != null) {
            return conversionFunction.apply(node);
        } else {
            throw new IllegalArgumentException("Unknown node type: " + node.toString());
        }
    }

    public static ObjectMapper getObjectMapper() {
        return JsonUtils.OBJECT_MAPPER;
    }

    public static CollectionType getListOfMapType() {
        return JsonUtils.LIST_OF_MAP_TYPE;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Map<String, Object> convertFlatMapToStructuredMap(Map<FieldKey, Object> rowData) {
        Map<String, Object> rootNode = new HashMap<>();
        for (Map.Entry<FieldKey, Object> entry : rowData.entrySet()) {
            String[] pathParts = entry.getKey().getKey().split("\\.");
            if (pathParts.length == 1) {
                rootNode.put(pathParts[0], entry.getValue());
            } else {
                Object currentNode = rootNode;
                for (int i = 0; i < pathParts.length; i++) {
                    String currPart = pathParts[i];
                    KeyIndexPair keyIndexPair = currPart.endsWith("]") ? new KeyIndexPair(currPart) : null;
                    if (currentNode instanceof HashMap) {
                        HashMap m = (HashMap) currentNode;
                        if (m.containsKey(currPart)) { // if object field key exists
                            currentNode = m.get(currPart);
                        } else if (keyIndexPair != null && m.containsKey(keyIndexPair.getKey())) { // if  array field key exists
                            currentNode = m.get(keyIndexPair.getKey()); // set current node as list
                            --i; // re-run loop to handle list
                        } else {
                            if (i == pathParts.length - 1) { // if end of the path
                                if (keyIndexPair == null) { // and if not array field key
                                    m.put(currPart, entry.getValue()); // just put value to the map
                                } else { // and if array field key, create a list  and put value to the list at specified index.
                                    ArrayList<Object> arr = new ArrayList<>();
                                    putElementToSpecifiedIndex(arr, keyIndexPair.getIndex(), entry.getValue());
                                    m.put(keyIndexPair.getKey(), arr);
                                    currentNode = arr;
                                }
                            } else { // if not end of the path
                                if (keyIndexPair == null) { // if not array field key, create a new map and put it to the map.
                                    Map<String, Object> newMap = new HashMap<>();
                                    m.put(currPart, newMap);
                                    currentNode = newMap;
                                } else { // if array field key, create a list and, set new map to specified index of list
                                    ArrayList<Object> arr = new ArrayList<>();
                                    Map<Object, Object> map = new HashMap<>();
                                    putElementToSpecifiedIndex(arr, keyIndexPair.getIndex(), map);
                                    m.put(keyIndexPair.getKey(), arr);
                                    currentNode = map;
                                }
                            }
                        }
                    } else if (currentNode instanceof ArrayList) {
                        ArrayList<Object> list = (ArrayList) currentNode;
                        if (keyIndexPair == null) continue; // this shouldn't happen but just in case
                        if (i == pathParts.length - 1) { // if end of the path, that means this is just a regular value array. put value to array than continue
                            putElementToSpecifiedIndex(list, keyIndexPair.getIndex(), entry.getValue());
                        } else { // and if not end of the path
                            if (list.size() <= keyIndexPair.getIndex() || list.get(keyIndexPair.getIndex()) == null) { // check map exists in index and if it's not than crate new map and set it to index
                                Map<Object, Object> map = new HashMap<>();
                                putElementToSpecifiedIndex(list, keyIndexPair.getIndex(), map);
                                currentNode = map;
                            } else { // if map exists than set current node as map in index.
                                currentNode = list.get(keyIndexPair.getIndex());
                            }
                        }
                    }
                }
            }
        }
        return rootNode;
    }

    private static void putElementToSpecifiedIndex(ArrayList<Object> arrayList, int index, Object val) {
        while (arrayList.size() < index + 1) {
            arrayList.add(null);
        }
        arrayList.set(index, val);
    }

    public static JsonNode convertStructuredMapToJsonNode(List<Map<String, Object>> listOfData) {
        return getObjectMapper().valueToTree(listOfData);
    }

    static {
        OBJECT_MAPPER = JsonMapper.builder()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .addModule(new JacksonXmlModule())
                .addModule(new JavaTimeModule())
                .addModule(new Jdk8Module())
                .addModule(new ParameterNamesModule())
                .addModule(new SimpleModule())
                .defaultPrettyPrinter(new DefaultPrettyPrinter())
                .build();

        LIST_OF_MAP_TYPE = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, Map.class);

        Map<JsonNodeType, Function<JsonNode, Object>> map = new EnumMap<>(JsonNodeType.class);
        map.put(JsonNodeType.NULL, n -> null);
        map.put(JsonNodeType.STRING, JsonNode::asText);
        map.put(JsonNodeType.BOOLEAN, JsonNode::booleanValue);
        map.put(JsonNodeType.NUMBER, n -> {
            if (n.isInt()) {
                return n.asInt();
            } else if (n.isFloatingPointNumber()) {
                return n.doubleValue();
            } else if (n.isBigInteger()) {
                return n.bigIntegerValue();
            } else {
                return null;
            }
        });
        NODE_CONVERSION_MAP = Collections.unmodifiableMap(map);
    }

    @Getter
    private static final class KeyIndexPair {
        private final String fullKey;
        private final String key;
        private final int    index;

        public KeyIndexPair(String fullKey) {
            this.fullKey = fullKey;
            Matcher m = SQUARE_BRACKET_PATTERN.matcher(fullKey);
            if (m.find()) {
                this.index = Integer.parseInt(m.group(0).substring(1, m.group(0).length() - 1));
                this.key = m.replaceAll("");
            } else {
                throw new IllegalArgumentException("Invalid key: " + fullKey);
            }
        }
    }

}
