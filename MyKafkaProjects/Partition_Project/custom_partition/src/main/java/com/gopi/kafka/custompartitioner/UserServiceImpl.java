package com.gopi.kafka.custompartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: Gopinathan Munappy
 *
 * @class:  UserServiceImpl class
 *
 */

public class UserServiceImpl implements IUserService {

  // Pairs of username and id
  private Map<String, Integer> usersMap;

  public UserServiceImpl() {
    usersMap = new HashMap<>();
    usersMap.put("Gopinathan", 1);
    usersMap.put("Ajith", 2);
    usersMap.put("Sreekumar", 3);
    usersMap.put("Nimmy", 4);
    usersMap.put("Joju", 5);
  }

  @Override
  public Integer findUserId(String userName) {

    return usersMap.get(userName);
  }


  @Override
  public List<String> findAllUsers() {

    return new ArrayList<>(usersMap.keySet());
  }

}
