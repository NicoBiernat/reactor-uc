#ifndef REACTOR_UC_EVENT_H
#define REACTOR_UC_EVENT_H

#include "reactor-uc/error.h"
#include "reactor-uc/tag.h"
#include "reactor-uc/platform.h"
#include <stdbool.h>

#define EVENT_INIT(Tag, Trigger, Payload)                                                                              \
  {.super.type = EVENT, .super.tag = Tag, .intended_tag = Tag, .trigger = Trigger, .super.payload = Payload}

#define SYSTEM_EVENT_INIT(Tag, Handler, Payload)                                                                       \
  {.super.type = SYSTEM_EVENT, .super.tag = Tag, .super.payload = Payload, .handler = Handler}

typedef struct Trigger Trigger;
typedef struct SystemEventHandler SystemEventHandler;
typedef struct EventPayloadPool EventPayloadPool;

typedef enum { EVENT, SYSTEM_EVENT } EventType;

/** Abstract event type which all other events inherit from. All event pointers can be casted to AbstractEvent. */
typedef struct {
  // `tag` is the first member: tag_t is 8-byte aligned, so this order avoids padding on 32-bit platforms.
  tag_t tag;
  EventType type;
  void* payload;
} AbstractEvent;

/** Normal reactor event. */
typedef struct {
  AbstractEvent super;
  tag_t intended_tag; // Intended tag used to catch STP violations in federated setting.
  Trigger* trigger;
} Event;

/** System events used to schedule system activities that are unordered wrt reactor events. */
typedef struct {
  AbstractEvent super;
  SystemEventHandler* handler;
} SystemEvent;

/** Arbitrary event is a structure with enough memory to hold any type of event.*/
typedef struct {
  union {
    Event event;
    SystemEvent system_event;
  };
} ArbitraryEvent;

struct EventPayloadPool {
  char* buffer;
  bool* used;
  /** Number of bytes per payload */
  size_t payload_size;
  /**  Max number of allocated payloads*/
  size_t capacity;
  /** Number of payloads reserved to be allocated through `allocate_reserved` */
  size_t reserved;

  MUTEX_T mutex;

  /** Allocate a payload from the pool. */
  lf_ret_t (*allocate)(EventPayloadPool* self, void** payload);
  lf_ret_t (*allocate_reserved)(EventPayloadPool* self, void** payload);
  /** Free a payload. */
  lf_ret_t (*free)(EventPayloadPool* self, void* payload);
};

/** Abstract base class for objects that can handle SystemEvents such as ClockSync and StartupCoordinator. */
struct SystemEventHandler {
  void (*handle)(SystemEventHandler* self, SystemEvent* event);
  EventPayloadPool payload_pool;
};

void EventPayloadPool_ctor(EventPayloadPool* self, char* buffer, bool* used, size_t element_size, size_t capacity,
                           size_t reserved);

/**
 * @brief Get the tag of an event.
 * @param event Pointer to the event (an Event or a SystemEvent).
 * @return The tag of the event.
 */
static inline tag_t get_tag(const AbstractEvent* event) { return event->tag; }

/**
 * @brief Compare the tags of two events.
 * @param evt1 Pointer to the first event.
 * @param evt2 Pointer to the second event.
 * @return true if the tags are the same, false otherwise.
 */
static inline bool events_same_tag(const AbstractEvent* evt1, const AbstractEvent* evt2) {
  return lf_tag_compare(get_tag(evt1), get_tag(evt2)) == 0;
}

/**
 * @brief Compare the triggers of two events.
 * @param evt1 Pointer to the first event.
 * @param evt2 Pointer to the second event.
 * @return true if both are (reactor) events with the same trigger, false otherwise.
 */
static inline bool events_same_trigger(const AbstractEvent* evt1, const AbstractEvent* evt2) {
  if (evt1->type != EVENT || evt2->type != EVENT) {
    return false;
  }
  return ((const Event*)evt1)->trigger == ((const Event*)evt2)->trigger;
}

/**
 * @brief Compare the handlers of two events.
 * @param evt1 Pointer to the first event.
 * @param evt2 Pointer to the second event.
 * @return true if both are system events with the same handler, false otherwise.
 */
static inline bool events_same_handler(const AbstractEvent* evt1, const AbstractEvent* evt2) {
  if (evt1->type != SYSTEM_EVENT || evt2->type != SYSTEM_EVENT) {
    return false;
  }
  return ((const SystemEvent*)evt1)->handler == ((const SystemEvent*)evt2)->handler;
}

/**
 * @brief Compare the tags and triggers of two events.
 * @param evt1 Pointer to the first event.
 * @param evt2 Pointer to the second event.
 * @return true if the tags and triggers of both events are the same, false otherwise.
 */
static inline bool events_same_tag_and_trigger(const AbstractEvent* evt1, const AbstractEvent* evt2) {
  return (events_same_tag(evt1, evt2) && events_same_trigger(evt1, evt2)) != 0;
}

/**
 * @brief Compare the tags and handlers of two events.
 * @param evt1 Pointer to the first event.
 * @param evt2 Pointer to the second event.
 * @return true if the tags and handlers of both events are the same, false otherwise.
 */
static inline bool events_same_tag_and_handler(const AbstractEvent* evt1, const AbstractEvent* evt2) {
  return (events_same_tag(evt1, evt2) && events_same_handler(evt1, evt2)) != 0;
}

#endif
