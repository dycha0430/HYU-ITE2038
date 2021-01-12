.SUFFIXS: .c .o

CC=gcc

SRCDIR=src/
UNIT_DIR=unittest/
INC=include/

# lock table test file
LT_SRC:=$(UNIT_DIR)unittest_lock_table.c
LT_OBJ:=$(LT_SRC:.c=.o)
LT_TARGET=unittest_lock_table

# DON'T TOUCH THIS !!!!!!!
MARKER_SRC:=$(UNIT_DIR)unittest_marker.c
MARKER_OBJ:=$(MARKER_SRC:.c=.o)
MARKER_TARGET=unittest_marker

# Include more files if you write another source file.
SRC:= \
	$(SRCDIR)lock_table.c

OBJ:=$(SRC:.c=.o)

CFLAGS+= -g -Wall -fPIC -I $(INC)

unit_lt: $(LT_TARGET)

$(LT_TARGET): $(LT_OBJ) $(OBJ)
	$(CC) $(CFLAGS) $< -o $@ $(OBJ) -lpthread

unit_marker: $(MARKER_TARGET)

$(MARKER_TARGET): $(MARKER_OBJ) $(OBJ)
	$(CC) $(CFLAGS) $< -o $@ $(OBJ) -lpthread

%.o: %.c
	$(CC) $(CFLAGS) $^ -c -o $@ -lpthread

clean:
	$(RM) $(OBJ)
	$(RM) $(TARGET) $(TARGET_OBJ)
	$(RM) $(LT_TARGET) $(LT_OBJ)
	$(RM) $(MARKER_TARGET) $(MARKER_OBJ)
