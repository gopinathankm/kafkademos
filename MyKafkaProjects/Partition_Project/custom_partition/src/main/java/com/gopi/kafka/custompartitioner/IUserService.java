package com.gopi.kafka.custompartitioner;

import java.util.List;

/**
 * @author: Gopinathan Munappy
 *
 * @interface:  IUserService interface
 *
 */

public interface IUserService {

  public Integer findUserId(String userName);
  public List<String> findAllUsers();

}
